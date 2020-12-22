/*
Copyright © 2020 A. Jensen <jensen.aaro@gmail.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/
package cmd

import (
	"cloud.google.com/go/logging"
	"context"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/sync/errgroup"
	"time"

	"github.com/spf13/cobra"

	"github.com/ajjensen13/gke"
)

const (
	dbSecretName  = "stocker-db-secret.json"
	appConfigName = "stocker-config-cm.json"
	apiSecretName = "stocker-api-secret.json"
)

// etlCmd represents the etl command
var etlCmd = &cobra.Command{
	Use:   "etl",
	Short: "runs a stocker etl",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		logger, cleanupLogger := logger()
		defer cleanupLogger()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := runEtl(ctx, cmd, logger)
		if err != nil {
			panic(err)
		}
	},
}

func runEtl(ctx context.Context, cmd *cobra.Command, logger gke.Logger) error {
	pool, poolCleanup, err := pool(ctx, logger)
	if err != nil {
		return err
	}
	defer poolCleanup()

	jobRunId, err := startJob(ctx, pool)
	if err != nil {
		return err
	}

	throttler := time.NewTicker(time.Second)
	defer throttler.Stop()

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ess, err := processStocks(grpCtx, cmd, logger, jobRunId, pool, throttler)
		if err != nil {
			return err
		}

		grp.Go(func() error {
			return processCandles(grpCtx, logger, jobRunId, pool, append([]finnhub.Stock{}, ess...), throttler)
		})

		grp.Go(func() error {
			return processCompanyProfiles(grpCtx, logger, jobRunId, pool, append([]finnhub.Stock{}, ess...), throttler)
		})

		return nil
	})

	errWait := grp.Wait()
	errEnd := endJob(ctx, pool, jobRunId, errWait == nil)
	if errEnd != nil {
		_ = logger.LogSync(ctx, logging.Entry{Severity: logging.Error, Payload: errEnd.Error()})
	}
	return errWait
}

func startJob(ctx context.Context, pool *pgxpool.Pool) (jobRunId uint64, err error) {
	var did uint64
	row := pool.QueryRow(ctx, `SELECT id FROM metadata.job_definition WHERE name = 'Finnhub ETL'`)
	err = row.Scan(&did)
	if err != nil {
		return 0, fmt.Errorf("failed to determine job definition id: %w", err)
	}

	row = pool.QueryRow(ctx, `INSERT INTO metadata.job_run (job_definition_id) VALUES ($1) RETURNING id`, did)
	err = row.Scan(&jobRunId)
	if err != nil {
		return 0, fmt.Errorf("failed to create new job run: %w", err)
	}

	return jobRunId, nil
}

func endJob(ctx context.Context, pool *pgxpool.Pool, jobRunId uint64, success bool) error {
	_, err := pool.Exec(ctx, `UPDATE metadata.job_run SET success = $1 WHERE id = $2`, success, jobRunId)
	if err != nil {
		return fmt.Errorf("failed to update job_run.success: %w", err)
	}

	return nil
}

func processStocks(ctx context.Context, cmd *cobra.Command, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, throttler *time.Ticker) ([]finnhub.Stock, error) {
	<-throttler.C
	ess, err := extractStocks(backoffContext(ctx, 5*time.Minute), lg)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve stocks from finnhub: %w", err)
	}
	lg.Info(gke.NewFmtMsgData("successfully extracted %d stocks", len(ess)))

	ess, err = skipAndLimit(cmd, lg, ess)
	if err != nil {
		return nil, fmt.Errorf("failed to reduce result set using skip and limit: %w", err)
	}

	err = loadStocks(backoffContext(ctx, 5*time.Minute), lg, jobRunId, pool, ess)
	if err != nil {
		return nil, fmt.Errorf("failed to load stocks into database: %w", err)
	}
	lg.Info(gke.NewFmtMsgData("successfully loaded %d stocks", len(ess)))

	si, err := stageStocks(backoffContext(ctx, 5*time.Minute), lg, jobRunId, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to stage stocks: %w", err)
	}
	lg.Infof("successfully staged %d stocks (%d rows modified)", si.RowsStaged, si.RowsModified)

	lg.Infof("committed database transaction for stocks")
	return ess, nil
}

func processCompanyProfiles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, ess []finnhub.Stock, throttler *time.Ticker) error {
	for _, es := range ess {
		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting company profile request %q from finnhub: %w", es.Symbol, ctx.Err())
		case <-throttler.C:
			ecp, err := extractCompanyProfile(backoffContext(ctx, 5*time.Minute), lg, es)
			if err != nil {
				_ = lg.ErrorErr(fmt.Errorf("failed to retrieve company profile %q from finnhub: %w", es.Symbol, err))
				_ = lg.Infof("company profile %q will be skipped due to error", es.Symbol)
				continue
			}

			if ecp.Ticker == "" {
				_ = lg.Warningf("company profile %q will be skipped due to missing ticker", es.Symbol)
				continue
			}

			err = loadCompanyProfile(backoffContext(ctx, 5*time.Minute), lg, jobRunId, pool, ecp)
			if err != nil {
				return fmt.Errorf("failed to load company profile %q into database: %w", es.Symbol, err)
			}

			lg.Defaultf("requested & loaded company profile from finnhub into database: %q", es.Symbol)
		}
	}

	si, err := stageCompanyProfiles(backoffContext(ctx, 5*time.Minute), lg, jobRunId, pool)
	if err != nil {
		return fmt.Errorf("failed to stage company profiles: %w", err)
	}
	lg.Defaultf("successfully staged %d company profiles (%d rows modified)", si.RowsStaged, si.RowsModified)

	lg.Infof("committed database transaction for company profiles")
	return nil
}

func processCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, ess []finnhub.Stock, throttler *time.Ticker) error {
	latest, err := queryMostRecentCandles(backoffContext(ctx, 5*time.Minute), lg, jobRunId, pool)
	if err != nil {
		return fmt.Errorf("failed to get latest stocks: %w", err)
	}
	lg.Default(gke.NewFmtMsgData("extracted %d existing candles from database", len(latest)))

	for _, es := range ess {
		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting candle request %q from finnhub: %w", es.Symbol, ctx.Err())
		case <-throttler.C:
			ec, err := extractCandles(backoffContext(ctx, 5*time.Minute), lg, es, latest)
			if err != nil {
				_ = lg.ErrorErr(fmt.Errorf("failed to retrieve stock candles %q from finnhub: %w", es.Symbol, err))
				_ = lg.Infof("stock candles %q will be skipped due to error", es.Symbol)
				continue
			}

			err = loadCandles(backoffContext(ctx, 5*time.Minute), lg, jobRunId, pool, ec)
			if err != nil {
				return fmt.Errorf("failed to load stock candles %q into database: %w", es.Symbol, err)
			}
			lg.Defaultf("requested & loaded %d stock candles from finnhub into database: %s", len(ec.StockCandles.T), es.Symbol)

			si, err := stageCandles(backoffContext(ctx, 5*time.Minute), lg, jobRunId, pool, es.Symbol)
			if err != nil {
				return fmt.Errorf("failed to stage candles for symbol %s: %w", es.Symbol, err)
			}
			lg.Defaultf("successfully staged %d candles for symbol %s", si.RowsStaged, es.Symbol)

			si, err = stage52WkCandles(backoffContext(ctx, 5*time.Minute), lg, jobRunId, pool, es.Symbol)
			if err != nil {
				return fmt.Errorf("failed to stage 52wk candles: %w", err)
			}
			lg.Defaultf("successfully staged %d 52wk candles (%d rows modified)", si.RowsStaged, si.RowsModified)
		}
	}
	return nil
}

type (
	MigrationSourceURL string
	Timezone           string
	DataSourceName     string
	Exchange           string
	Resolution         string
)

type appConfig struct {
	Exchange           Exchange           `json:"exchange"`
	Resolution         Resolution         `json:"resolution"`
	StartDate          time.Time          `json:"startDate"`
	EndDate            time.Time          `json:"endDate"`
	DataSourceName     DataSourceName     `json:"dataSourceName"`
	DbConnPoolConfig   dbConnPoolConfig   `json:"dbConnPoolConfig"`
	Timezone           Timezone           `json:"timezone"`
	MigrationSourceURL MigrationSourceURL `json:"migrationSourceUrl"`
}

type appSecrets struct {
	ApiKey string `json:"api_key"`
}

type dbConnPoolConfig struct {
	MaxConnLifetime   string `json:"maxConnLifetime"`
	MaxConnIdleTime   string `json:"maxConnIdleTime"`
	HealthCheckPeriod string `json:"healthCheckPeriod"`
	MinConns          int    `json:"minConns"`
	MaxConns          int    `json:"maxConns"`
}

func skipAndLimit(cmd *cobra.Command, lg gke.Logger, ess []finnhub.Stock) ([]finnhub.Stock, error) {
	reqS, err := cmd.Flags().GetInt("skip")
	if err != nil {
		return nil, fmt.Errorf("failed to get skip flag: %w", err)
	}
	var actS = 0
	if reqS > -1 {
		actS = reqS
		maxS := len(ess)
		if reqS > maxS {
			actS = reqS
		}
		lg.Defaultf("skipping %d entries (skip %d requested, %d entries found)", actS, reqS, maxS)
		ess = ess[actS:]
	}

	reqL, err := cmd.Flags().GetInt("limit")
	if err != nil {
		return nil, fmt.Errorf("failed to get limit flag: %w", err)
	}
	var actL = 0
	if reqL > -1 {
		actL = reqL
		maxL := len(ess)
		if reqL > maxL {
			actL = reqL
		}
		lg.Defaultf("limiting to %d entries (limit %d requested, %d entries remain after skipping %d)", actL, reqL, maxL, actS)
		ess = ess[:actL]
	}
	return ess, nil
}

func init() {
	rootCmd.AddCommand(etlCmd)
	etlCmd.Flags().IntP("skip", "s", -1, "number of stocks to skip")
	etlCmd.Flags().IntP("limit", "l", -1, "maximum number of stocks to update")
}

func backoffContext(ctx context.Context, maxElapsedTime time.Duration) backoff.BackOffContext {
	result := backoff.NewExponentialBackOff()
	result.InitialInterval = time.Second
	result.MaxElapsedTime = maxElapsedTime
	return backoff.WithContext(result, ctx)
}

type latestStock struct {
	symbol    string
	timestamp time.Time
}

type candleConfig struct {
	resolution Resolution
	startDate  time.Time
	endDate    time.Time
}

type latestStocks map[string]time.Time

type apiAuthContext context.Context

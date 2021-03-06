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
	"github.com/ajjensen13/stocker/internal/api"
	"github.com/ajjensen13/stocker/internal/util"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/sync/errgroup"
	"time"

	"github.com/spf13/cobra"
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
		time.Sleep(time.Second * 5)

		logger, cleanupLogger := logger()
		defer cleanupLogger()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = util.WithLogger(ctx, logger)

		err := runEtl(ctx, cmd)
		if err != nil {
			panic(err)
		}
	},
}

func runEtl(ctx context.Context, cmd *cobra.Command) error {
	util.Logf(ctx, logging.Notice, "ETL is starting")
	defer util.Logf(ctx, logging.Notice, "ETL is stopping")

	pool, poolCleanup, err := pool(ctx)
	if err != nil {
		return err
	}
	defer poolCleanup()

	jobRunId, err := startJob(ctx, pool)
	if err != nil {
		return err
	}

	ctx = util.WithLoggerValue(ctx, "job_run_id", fmt.Sprintf("job_run_%d", jobRunId))

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		stocks, err := processStocks(grpCtx, jobRunId, pool)
		if err != nil {
			return err
		}

		grp.Go(func() error {
			return processCandles(grpCtx, jobRunId, pool, stocks)
		})

		grp.Go(func() error {
			return processCompanyProfiles(grpCtx, jobRunId, pool, stocks)
		})

		return nil
	})

	errWait := grp.Wait()
	errEnd := endJob(ctx, pool, jobRunId, errWait == nil)
	if errEnd != nil {
		util.Logf(ctx, logging.Error, errEnd.Error())
	}
	return errWait
}

func cleanupSrcSchema(ctx context.Context, pool *pgxpool.Pool) error {
	for _, table := range []string{"stocks", "company_profiles", "candles"} {
		_, err := pool.Exec(ctx, fmt.Sprintf("truncate table src.%s", table))
		if err != nil {
			return fmt.Errorf("failed to truncate src.%s: %w", table, err)
		}
	}
	return nil
}

func startJob(ctx context.Context, pool *pgxpool.Pool) (jobRunId uint64, err error) {
	err = cleanupSrcSchema(ctx, pool)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup src schema: %w", err)
	}
	util.Logf(ctx, logging.Debug, "successfully cleaned up the src schema")

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

func processStocks(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool) (api.StocksResponse, error) {
	ctx = util.WithLoggerValue(ctx, "action", "process")
	ctx = util.WithLoggerValue(ctx, "type", "stock")

	stocks, err := requestStocks(backoffContext(ctx, 5*time.Minute))
	if err != nil {
		return api.StocksResponse{}, fmt.Errorf("failed to retrieve stocks from finnhub: %w", err)
	}
	util.Logf(ctx, logging.Info, "successfully received %d stocks from finnhub", len(stocks.Response))

	err = saveStocks(backoffContext(ctx, 5*time.Minute), jobRunId, pool, stocks)
	if err != nil {
		return api.StocksResponse{}, fmt.Errorf("failed to load stocks into database: %w", err)
	}
	util.Logf(ctx, logging.Info, "successfully loaded %d stocks into src schema", len(stocks.Response))

	info, err := stageStocks(backoffContext(ctx, 5*time.Minute), jobRunId, pool)
	if err != nil {
		return api.StocksResponse{}, fmt.Errorf("failed to stage stocks: %w", err)
	}
	util.Logf(ctx, logging.Info, "successfully staged %d stocks into stage schema (%d rows modified)", info.RowsStaged, info.RowsModified)

	return stocks, nil
}

func processCompanyProfiles(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, stocks api.StocksResponse) error {
	ctx = util.WithLoggerValue(ctx, "type", "company_profile")

	success := 0
	for _, stock := range stocks.Response {
		ctx := util.WithLoggerValue(ctx, "symbol", stock.Symbol)

		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting company profile request %q from finnhub: %w", stock.Symbol, ctx.Err())
		default:
			profile, err := requestCompanyProfile(backoffContext(ctx, 5*time.Minute), api.Symbol(stock.Symbol))
			if err != nil {
				util.Logf(ctx, logging.Warning, "failed to retrieve company profile %q from finnhub: %v", stock.Symbol, err)
				continue
			}
			util.Logf(ctx, logging.Debug, "successfully retrieved %q company profile from finnhub", stock.Symbol)

			err = saveCompanyProfile(backoffContext(ctx, 5*time.Minute), jobRunId, pool, profile)
			if err != nil {
				return fmt.Errorf("failed to load company profile %q into database: %w", stock.Symbol, err)
			}
			util.Logf(ctx, logging.Debug, "successfully loaded %q company profile into src schema", stock.Symbol)

			success++
		}
	}
	util.Logf(ctx, logging.Info, "successfully loaded %d of %d company profiles into src schema", success, len(stocks.Response))

	info, err := stageCompanyProfiles(backoffContext(ctx, 5*time.Minute), jobRunId, pool)
	if err != nil {
		return fmt.Errorf("failed to stage company profiles: %w", err)
	}
	util.Logf(ctx, logging.Info, "successfully staged %d company profiles into stage schema (%d rows modified)", info.RowsStaged, info.RowsModified)

	return nil
}

func processCandles(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, stocks api.StocksResponse) error {
	ctx = util.WithLoggerValue(ctx, "type", "candle")

	latest, err := queryMostRecentCandles(backoffContext(ctx, 5*time.Minute), jobRunId, pool)
	if err != nil {
		return fmt.Errorf("failed to get latest stocks: %w", err)
	}
	util.Logf(ctx, logging.Info, "extracted %d existing candles from database", len(latest))

	for _, stock := range stocks.Response {
		ctx := util.WithLoggerValue(ctx, "symbol", stock.Symbol)

		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting candle request %q from finnhub: %w", stock.Symbol, ctx.Err())
		default:
			candles, err := requestCandles(backoffContext(ctx, 5*time.Minute), api.Symbol(stock.Symbol), latest)
			if err != nil {
				util.Logf(ctx, logging.Error, "failed to retrieve stock candles %q from finnhub: %v", stock.Symbol, err)
				continue
			}

			err = saveCandles(backoffContext(ctx, 5*time.Minute), jobRunId, pool, candles)
			if err != nil {
				return fmt.Errorf("failed to load stock candles %q into database: %w", stock.Symbol, err)
			}
			util.Logf(ctx, logging.Info, "requested & loaded %d stock candles from finnhub into database: %s", len(candles.Response.T), stock.Symbol)

			info, err := stageCandles(backoffContext(ctx, 5*time.Minute), jobRunId, pool, candles)
			if err != nil {
				return fmt.Errorf("failed to stage candles for symbol %s: %w", stock.Symbol, err)
			}
			util.Logf(ctx, logging.Info, "successfully staged %d candles for symbol %s", info.RowsStaged, stock.Symbol)

			var ctx = util.WithLoggerValue(ctx, "type", "52wk_candle")
			info, err = stage52WkCandles(backoffContext(ctx, 5*time.Minute), jobRunId, pool, candles)
			if err != nil {
				return fmt.Errorf("failed to stage 52wk candles: %w", err)
			}
			util.Logf(ctx, logging.Info, "successfully staged %d 52wk candles (%d rows modified)", info.RowsStaged, info.RowsModified)
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

type apiAuthContext context.Context

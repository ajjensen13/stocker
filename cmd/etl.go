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
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/sync/errgroup"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ajjensen13/gke"

	"github.com/ajjensen13/stocker/internal/model"
	"github.com/ajjensen13/stocker/internal/transform"
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
		err := runEtl(cmd)
		if err != nil {
			os.Exit(2)
		}
	},
}

func runEtl(cmd *cobra.Command) error {
	logger, cleanupLogger := logger()
	defer cleanupLogger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := func(ctx context.Context) error {
		pool, poolCleanup, err := openPool(ctx)
		if err != nil {
			return err
		}
		defer poolCleanup()

		throttler := time.NewTicker(time.Second)
		defer throttler.Stop()

		grp, grpCtx := errgroup.WithContext(ctx)
		grp.Go(func() error {
			ess, err := processStocks(grpCtx, cmd, logger, pool, throttler)
			if err != nil {
				return err
			}

			grp.Go(func() error {
				return processCandles(grpCtx, logger, pool, append([]finnhub.Stock{}, ess...), throttler)
			})

			grp.Go(func() error {
				return processCompanyProfiles(grpCtx, logger, pool, append([]finnhub.Stock{}, ess...), throttler)
			})

			return nil
		})

		return grp.Wait()
	}(ctx)

	if err != nil {
		err2 := logger.LogSync(ctx, logging.Entry{Severity: logging.Error, Payload: err.Error()})
		if err2 != nil {
			panic(err2)
		}
	}

	return err
}

func processStocks(ctx context.Context, cmd *cobra.Command, lg gke.Logger, pool *pgxpool.Pool, throttler *time.Ticker) ([]finnhub.Stock, error) {
	tx, err := openTx(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to setup database transaction for stock processing: %w", err)
	}

	<-throttler.C
	ess, err := extractStocks(ctx, lg)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve stocks from finnhub: %w", err)
	}
	lg.Info(gke.NewFmtMsgData("successfully extracted %d stocks", len(ess)))

	ess, err = skipAndLimit(cmd, lg, ess)
	if err != nil {
		return nil, fmt.Errorf("failed to reduce result set using skip and limit: %w", err)
	}

	tss := make([]model.Stock, 0, len(ess))
	for _, es := range ess {
		if es.Symbol == "" {
			lg.Warningf("skipping stock without symbol: %v", es)
			continue
		}
		tss = append(tss, transform.Stock(es))
	}

	err = loadStocks(ctx, lg, tx, tss)
	if err != nil {
		return nil, fmt.Errorf("failed to load stocks into database: %w", err)
	}
	lg.Info(gke.NewFmtMsgData("successfully loaded %d stocks", len(tss)))

	si, err := stageStocks(ctx, lg, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to stage stocks: %w", err)
	}
	lg.Infof("successfully staged %d stocks (previous latest modification: %v)", si.RowsAffected, si.PreviousLatestModification)

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error while committing stock processing database transaction: %w", err)
	}

	lg.Infof("committed database transaction for stocks")
	return ess, nil
}

func processCompanyProfiles(ctx context.Context, lg gke.Logger, pool *pgxpool.Pool, ess []finnhub.Stock, throttler *time.Ticker) error {
	tx, err := openTx(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to setup database transaction for company profile processing: %w", err)
	}

	pingTickInterval := calcPingTickInterval(pool)
	pingTicker := time.NewTicker(pingTickInterval)
	defer pingTicker.Stop()

	for _, es := range ess {
	top:
		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting company profile request %q from finnhub: %w", es.Symbol, ctx.Err())
		case <-pingTicker.C:
			// Prevent this connection from being cleaned up by being idle.
			err := tx.Conn().Ping(ctx)
			if err != nil {
				lg.Warningf("pinging database failed: %w", err)
			}
			goto top
		case <-throttler.C:
			pingTicker.Reset(pingTickInterval)
			ecp, err := extractCompanyProfile(ctx, lg, es)
			if err != nil {
				_ = lg.ErrorErr(fmt.Errorf("failed to retrieve company profile %q from finnhub: %w", es.Symbol, err))
				_ = lg.Infof("company profile %q will be skipped due to error", es.Symbol)
				continue
			}

			tcp, err := transformCompanyProfile(ecp)
			if err != nil {
				return fmt.Errorf("failed to transform company profile %q: %w", es.Symbol, err)
			}

			err = loadCompanyProfile(ctx, lg, tx, tcp)
			if err != nil {
				return fmt.Errorf("failed to load company profile %q into database: %w", es.Symbol, err)
			}

			lg.Defaultf("requested & loaded company profile from finnhub into database: %q", es.Symbol)
		}
	}

	si, err := stageCompanyProfiles(ctx, lg, tx)
	if err != nil {
		return fmt.Errorf("failed to stage company profiles: %w", err)
	}
	lg.Defaultf("successfully staged %d company profiles (previous latest modification: %v)", si.RowsAffected, si.PreviousLatestModification)

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("error while committing company profile processing database transaction: %w", err)
	}

	lg.Infof("committed database transaction for company profiles")
	return nil
}

func processCandles(ctx context.Context, lg gke.Logger, pool *pgxpool.Pool, ess []finnhub.Stock, throttler *time.Ticker) error {
	tx, err := openTx(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to setup database transaction for candles processing: %w", err)
	}

	latest, err := queryMostRecentCandles(ctx, lg, tx)
	if err != nil {
		return fmt.Errorf("failed to get latest stocks: %w", err)
	}
	lg.Default(gke.NewFmtMsgData("extracted %d existing candles from database", len(latest)))

	tz, err := timezone()
	if err != nil {
		return fmt.Errorf("failed to get timezone: %w", err)
	}
	lg.Default(gke.NewFmtMsgData("using %v timezone to store data", tz))

	pingTickInterval := calcPingTickInterval(pool)
	pingTicker := time.NewTicker(pingTickInterval)
	defer pingTicker.Stop()

	for _, es := range ess {
	top:
		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting candle request %q from finnhub: %w", es.Symbol, ctx.Err())
		case <-pingTicker.C:
			// Prevent this connection from being cleaned up by being idle.
			err := tx.Conn().Ping(ctx)
			if err != nil {
				lg.Warningf("pinging database failed: %w", err)
			}
			goto top
		case <-throttler.C:
			pingTicker.Reset(pingTickInterval)
			ec, err := extractCandles(ctx, lg, es, latest)
			if err != nil {
				_ = lg.ErrorErr(fmt.Errorf("failed to retrieve stock candles %q from finnhub: %w", es.Symbol, err))
				_ = lg.Infof("stock candles %q will be skipped due to error", es.Symbol)
				continue
			}

			tcs, err := transformCandles(es, ec, tz)
			if err != nil {
				return fmt.Errorf("failed to transform stock candles %q: %w", es.Symbol, err)
			}

			err = loadCandles(ctx, lg, tx, tcs)
			if err != nil {
				return fmt.Errorf("failed to load stock candles %q into database: %w", es.Symbol, err)
			}

			si, err := stageCandles(ctx, lg, tx, es.Symbol)
			if err != nil {
				return fmt.Errorf("failed to stage candles for symbol %s: %w", es.Symbol, err)
			}
			lg.Defaultf("successfully staged %d candles for symbol %s (previous latest modification: %v)", si.RowsAffected, es.Symbol, si.PreviousLatestModification)

			lg.Defaultf("requested & loaded %d stock candles from finnhub into database: %s", len(tcs), es.Symbol)
			si, err = stage52WkCandles(ctx, lg, tx, es.Symbol, si.PreviousLatestModification)
			if err != nil {
				return fmt.Errorf("failed to stage candles: %w", err)
			}
			lg.Defaultf("successfully staged %d 52wk candles (previous latest modification: %v)", si.RowsAffected, si.PreviousLatestModification)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("error while committing candle processing database transaction: %w", err)
	}

	lg.Infof("committed database transaction for candles")
	return nil
}

type appConfig struct {
	Exchange           string           `json:"exchange"`
	Resolution         string           `json:"resolution"`
	StartDate          time.Time        `json:"start_date"`
	EndDate            time.Time        `json:"end_date"`
	DataSourceName     string           `json:"data_source_name"`
	DbConnPoolConfig   dbConnPoolConfig `json:"db_conn_pool_config"`
	Timezone           string           `json:"timezone"`
	MigrationSourceURL string           `json:"migration_source_url"`
}

type appSecrets struct {
	ApiKey string `json:"api_key"`
}

type dbConnPoolConfig struct {
	MaxConnLifetime   string `json:"max_conn_lifetime"`
	MaxConnIdleTime   string `json:"max_conn_idle_time"`
	HealthCheckPeriod string `json:"health_check_period"`
	MinConns          int    `json:"min_conns"`
	MaxConns          int    `json:"max_conns"`
}

func calcPingTickInterval(pool *pgxpool.Pool) time.Duration {
	poolCfg := pool.Config()

	pingTickInterval := time.Minute
	if healthCheckPeriod := poolCfg.HealthCheckPeriod; healthCheckPeriod < pingTickInterval {
		pingTickInterval = healthCheckPeriod
	}

	if maxConnIdleTime := poolCfg.MaxConnIdleTime; maxConnIdleTime < pingTickInterval {
		pingTickInterval = maxConnIdleTime
	}

	if maxConnLifetime := poolCfg.MaxConnLifetime; maxConnLifetime < pingTickInterval {
		pingTickInterval = maxConnLifetime
	}

	return pingTickInterval
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

type latestStock struct {
	symbol    string
	timestamp time.Time
}

type candleConfig struct {
	resolution string
	startDate  time.Time
	endDate    time.Time
}

type latestStocks map[string]time.Time

type apiAuthContext context.Context

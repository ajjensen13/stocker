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
	"context"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/config"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"net/url"
	"time"

	"github.com/spf13/cobra"

	"github.com/ajjensen13/gke"

	"github.com/ajjensen13/stocker/internal/extract"
	"github.com/ajjensen13/stocker/internal/load"
	"github.com/ajjensen13/stocker/internal/model"
	"github.com/ajjensen13/stocker/internal/transform"
)

const (
	dbSecretName  = "stocker-db-secret.json"
	appConfigName = "stocker-config-cm.json"
	apiSecretName = "stocker-api-secret.json"
)

type appConfig struct {
	Exchange       string    `json:"exchange"`
	Resolution     string    `json:"resolution"`
	OverrideDate   time.Time `json:"override_date"`
	DataSourceName string    `json:"data_source_name"`
}

type appSecrets struct {
	ApiKey string `json:"api_key"`
}

// etlCmd represents the etl command
var etlCmd = &cobra.Command{
	Use:   "etl",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		lg, cleanup, err := gke.NewLogger(context.Background())
		if err != nil {
			panic(err)
		}
		defer cleanup()

		gke.LogEnv(lg)
		gke.LogMetadata(lg)

		tx, cleanup, err := openTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to setup database transaction: %w", err)))
		}
		defer cleanup()

		ctx := context.Background()
		ess, err := extractStocks(ctx, lg)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to retrieve stocks from finnhub: %w", err)))
		}

		latest, err := extractLatestStocks(ctx, lg, tx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to get latest stocks: %w", err)))
		}
		lg.Default(gke.NewFmtMsgData("extracted %d existing stocks from database", len(latest)))

		tss := make([]model.Stock, 0, len(ess))
		for _, es := range ess {
			if es.Symbol == "" {
				lg.Warningf("skipping stock without symbol: %v", es)
				continue
			}
			tss = append(tss, transform.Stock(es))
		}

		err = load.Stocks(ctx, tx, tss)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to load stocks into database: %w", err)))
		}
		lg.Default(gke.NewFmtMsgData("extracted & loaded %d stocks", len(tss)))

		throttler := time.NewTicker(time.Second)
		defer throttler.Stop()

		<-throttler.C
		for _, es := range ess {
			select {
			case <-ctx.Done():
				panic(lg.WarningErr(fmt.Errorf("aborting candle request %q from finnhub: %w", es.Symbol, ctx.Err())))
			case <-throttler.C:
				ec, err := extractCandles(ctx, lg, es, latest)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to retrieve stock candles %q from finnhub: %w", es.Symbol, err)))
				}

				tcs, err := transform.Candles(es, ec)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to transform stock candles %q: %w", es.Symbol, err)))
				}

				err = load.Candles(ctx, tx, tcs)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to load stock candles %q into database: %w", es.Symbol, err)))
				}

				lg.Defaultf("requested & loaded %d stock candles from finnhub into database: %s", len(tcs), es.Symbol)
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("error while committing database transaction: %w", err)))
		}

		lg.Defaultf("committed database transaction")
	},
}

func init() {
	rootCmd.AddCommand(etlCmd)
}

func provideAppSecrets() (*appSecrets, error) {
	var result appSecrets
	err := config.InterfaceJson(apiSecretName, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func provideApiServiceClient() *finnhub.DefaultApiService {
	return finnhub.NewAPIClient(finnhub.NewConfiguration()).DefaultApi
}

func provideApiAuthContext(ctx context.Context, secrets *appSecrets) apiAuthContext {
	return context.WithValue(ctx, finnhub.ContextAPIKey, finnhub.APIKey{Key: secrets.ApiKey})
}

func provideAppConfig() (*appConfig, error) {
	var result appConfig
	err := config.InterfaceJson(appConfigName, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func provideDbSecrets() (*url.Userinfo, error) {
	ui, err := config.Userinfo(dbSecretName)
	if err != nil {
		return nil, err
	}
	return ui, nil
}

func provideBackoff() backoff.BackOff {
	result := backoff.NewExponentialBackOff()
	result.InitialInterval = time.Second
	return result
}

type latestStock struct {
	symbol    string
	timestamp time.Time
}

func provideLatestStock(stock finnhub.Stock, latest latestStocks) latestStock {
	return latestStock{
		symbol:    stock.Symbol,
		timestamp: latest[stock.Symbol],
	}
}

func provideCandleConfig(cfg *appConfig, latest latestStock) candleConfig {
	var to time.Time
	if cfg.OverrideDate.IsZero() {
		now := time.Now()
		to = time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.Local)
	} else {
		to = cfg.OverrideDate
	}

	from := latest.timestamp.Add(time.Second)
	if to.Before(from) {
		to, from = from, to
	}

	return candleConfig{
		resolution: cfg.Resolution,
		from:       from,
		to:         to,
	}
}

type candleConfig struct {
	resolution string
	from       time.Time
	to         time.Time
}

func provideCandles(ctx apiAuthContext, lg gke.Logger, client *finnhub.DefaultApiService, bo backoff.BackOff, s finnhub.Stock, cfg candleConfig) (finnhub.StockCandles, error) {
	lg.Default(gke.NewMsgData(fmt.Sprintf("requesting %q candles from finnhub. (%v — %v) / %s", s.Symbol, cfg.from, cfg.to, cfg.resolution),
		struct {
			Symbol     string
			From, To   time.Time
			Resolution string
		}{s.Symbol, cfg.from, cfg.to, cfg.resolution}))
	return extract.Candles(ctx, client, bo, s, cfg.resolution, cfg.from, cfg.to)
}

func provideStocks(ctx apiAuthContext, lg gke.Logger, client *finnhub.DefaultApiService, bo backoff.BackOff, cfg *appConfig) ([]finnhub.Stock, error) {
	lg.Defaultf("requesting %s stocks from finnhub", cfg.Exchange)
	return extract.Stocks(ctx, client, bo, cfg.Exchange)
}

type latestStocks map[string]time.Time

func provideLatestStocks(latest map[string]time.Time) latestStocks {
	return latestStocks(latest)
}

func provideDbConnPool(ctx context.Context, user *url.Userinfo, cfg *appConfig) (*pgxpool.Pool, func(), error) {
	dsn, err := url.Parse(cfg.DataSourceName)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to parse data source name: %w", err)
	}
	dsn.User = user

	pool, err := pgxpool.Connect(ctx, dsn.String())
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to open database connection pool: %w", err)
	}

	return pool, pool.Close, nil
}

func provideDbConn(ctx context.Context, pool *pgxpool.Pool) (*pgx.Conn, func(), error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to aquire database connection: %w", err)
	}

	result := conn.Conn()
	err = result.Ping(ctx)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to ping database: %w", err)
	}

	return result, conn.Release, nil
}

func provideDbTx(ctx context.Context, conn *pgx.Conn, opts pgx.TxOptions) (pgx.Tx, error) {
	return conn.BeginTx(ctx, opts)
}

type apiAuthContext context.Context
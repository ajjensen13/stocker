// +build wireinject

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
	"github.com/google/wire"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"net/url"
	"time"

	"github.com/ajjensen13/stocker/internal/extract"
)

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

func provideCandleConfig(cfg *appConfig) candleConfig {
	var to time.Time
	if cfg.OverrideDate.IsZero() {
		now := time.Now()
		to = time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.Local)
	} else {
		to = cfg.OverrideDate
	}

	from := to.AddDate(0, 0, -cfg.LookBackDays)

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

func provideCandles(ctx apiAuthContext, client *finnhub.DefaultApiService, bo backoff.BackOff, s finnhub.Stock, cfg candleConfig) (finnhub.StockCandles, error) {
	return extract.Candles(ctx, client, bo, s, cfg.resolution, cfg.from, cfg.to)
}

func provideStocks(ctx apiAuthContext, client *finnhub.DefaultApiService, bo backoff.BackOff, cfg *appConfig) ([]finnhub.Stock, error) {
	return extract.Stocks(ctx, client, bo, cfg.Exchange)
}

func extractStocks(ctx context.Context) ([]finnhub.Stock, error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideAppConfig, provideBackoff, provideApiAuthContext, provideStocks))
}

func extractCandles(ctx context.Context, stock finnhub.Stock) (finnhub.StockCandles, error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideAppConfig, provideBackoff, provideApiAuthContext, provideCandles, provideCandleConfig))
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

	return pool, func() { pool.Close() }, nil
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

	return result, func() { _ = result.Close(context.TODO()) }, nil
}

func provideDbTx(ctx context.Context, conn *pgx.Conn, opts pgx.TxOptions) (pgx.Tx, error) {
	return conn.BeginTx(ctx, opts)
}

func openTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, func(), error) {
	panic(wire.Build(provideDbConnPool, provideDbConn, provideDbTx, provideDbSecrets, provideAppConfig))
}

type apiAuthContext context.Context

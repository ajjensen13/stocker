// Code generated by Wire. DO NOT EDIT.

//go:generate wire
//+build !wireinject

package cmd

import (
	"context"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/config"
	"github.com/ajjensen13/gke"
	"github.com/ajjensen13/stocker/internal/extract"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"net/url"
	"time"
)

// Injectors from wire.go:

func extractStocks(ctx context.Context) ([]finnhub.Stock, error) {
	cmdAppSecrets, err := provideAppSecrets()
	if err != nil {
		return nil, err
	}
	cmdApiAuthContext := provideApiAuthContext(ctx, cmdAppSecrets)
	defaultApiService := provideApiServiceClient()
	backOff := provideBackoff()
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return nil, err
	}
	v, err := provideStocks(cmdApiAuthContext, defaultApiService, backOff, cmdAppConfig)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func extractCandles(ctx context.Context, stock finnhub.Stock) (finnhub.StockCandles, error) {
	cmdAppSecrets, err := provideAppSecrets()
	if err != nil {
		return finnhub.StockCandles{}, err
	}
	cmdApiAuthContext := provideApiAuthContext(ctx, cmdAppSecrets)
	defaultApiService := provideApiServiceClient()
	backOff := provideBackoff()
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return finnhub.StockCandles{}, err
	}
	cmdCandleConfig := provideCandleConfig(cmdAppConfig)
	stockCandles, err := provideCandles(cmdApiAuthContext, defaultApiService, backOff, stock, cmdCandleConfig)
	if err != nil {
		return finnhub.StockCandles{}, err
	}
	return stockCandles, nil
}

func openTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, func(), error) {
	userinfo, err := provideDbSecrets()
	if err != nil {
		return nil, nil, err
	}
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return nil, nil, err
	}
	pool, cleanup, err := provideDbConnPool(ctx, userinfo, cmdAppConfig)
	if err != nil {
		return nil, nil, err
	}
	conn, cleanup2, err := provideDbConn(ctx, pool)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	tx, err := provideDbTx(ctx, conn, opts)
	if err != nil {
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	return tx, func() {
		cleanup2()
		cleanup()
	}, nil
}

// wire.go:

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

func provideCandleExtractor(ctx context.Context, lg gke.Logger, s finnhub.Stock, tx pgx.Tx) func(ctx context.Context) error {
	return func(ctx context.Context) error {

		return nil
	}
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

type apiAuthContext context.Context

// Code generated by Wire. DO NOT EDIT.

//go:generate wire
//+build !wireinject

package cmd

import (
	"context"
	"github.com/ajjensen13/gke"
	"github.com/ajjensen13/stocker/internal/api"
	db2 "github.com/ajjensen13/stocker/internal/db"
	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
)

import (
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// Injectors from wire.go:

func requestStocks(ctx backoff.BackOffContext) (api.StocksResponse, error) {
	context := provideContext(ctx)
	cmdAppSecrets, err := provideAppSecrets()
	if err != nil {
		return api.StocksResponse{}, err
	}
	cmdApiAuthContext := provideApiAuthContext(context, cmdAppSecrets)
	defaultApiService := provideApiServiceClient()
	ticker := _wireTickerValue
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return api.StocksResponse{}, err
	}
	exchange := cmdAppConfig.Exchange
	stocksRequest := buildStocksRequest(exchange)
	stocksResponse, err := requestStocksImpl(cmdApiAuthContext, defaultApiService, ticker, backOff, notify, stocksRequest)
	if err != nil {
		return api.StocksResponse{}, err
	}
	return stocksResponse, nil
}

var (
	_wireTickerValue = clientTicker
)

func saveStocks(ctx backoff.BackOffContext, jobRunId uint64, pool2 *pgxpool.Pool, resp api.StocksResponse) error {
	context := provideContext(ctx)
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	error2 := db2.SaveStocks(context, pool2, jobRunId, backOff, notify, resp)
	return error2
}

func stageStocks(ctx backoff.BackOffContext, jobRunId uint64, pool2 *pgxpool.Pool) (db2.StagingInfo, error) {
	context := provideContext(ctx)
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	stagingInfo, err := db2.StageStocks(context, pool2, jobRunId, backOff, notify)
	if err != nil {
		return db2.StagingInfo{}, err
	}
	return stagingInfo, nil
}

func requestCandles(ctx backoff.BackOffContext, symbol api.Symbol, lc db2.LatestCandles) (api.CandlesResponse, error) {
	context := provideContext(ctx)
	cmdAppSecrets, err := provideAppSecrets()
	if err != nil {
		return api.CandlesResponse{}, err
	}
	cmdApiAuthContext := provideApiAuthContext(context, cmdAppSecrets)
	defaultApiService := provideApiServiceClient()
	ticker := _wireTickerValue
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return api.CandlesResponse{}, err
	}
	latestCandleTime := latestCandleTimeFromLatestCandles(symbol, lc)
	location, err := provideTimezone(cmdAppConfig)
	if err != nil {
		return api.CandlesResponse{}, err
	}
	candlesRequest := buildCandleRequest(cmdAppConfig, latestCandleTime, location, symbol)
	candlesResponse, err := requestCandlesImpl(cmdApiAuthContext, defaultApiService, ticker, backOff, notify, candlesRequest)
	if err != nil {
		return api.CandlesResponse{}, err
	}
	return candlesResponse, nil
}

func saveCandles(ctx backoff.BackOffContext, jobRunId uint64, pool2 *pgxpool.Pool, resp api.CandlesResponse) error {
	context := provideContext(ctx)
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	error2 := db2.SaveCandles(context, jobRunId, pool2, backOff, notify, resp)
	return error2
}

func stageCandles(ctx backoff.BackOffContext, jobRunId uint64, pool2 *pgxpool.Pool, resp api.CandlesResponse) (db2.StagingInfo, error) {
	context := provideContext(ctx)
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return db2.StagingInfo{}, err
	}
	location, err := provideTimezone(cmdAppConfig)
	if err != nil {
		return db2.StagingInfo{}, err
	}
	stagingInfo, err := db2.StageCandles(context, jobRunId, pool2, backOff, notify, location)
	if err != nil {
		return db2.StagingInfo{}, err
	}
	return stagingInfo, nil
}

func stage52WkCandles(ctx backoff.BackOffContext, jobRunId uint64, pool2 *pgxpool.Pool, resp api.CandlesResponse) (db2.StagingInfo, error) {
	context := provideContext(ctx)
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	stagingInfo, err := db2.StageCandles52Wk(context, jobRunId, pool2, backOff, notify, resp)
	if err != nil {
		return db2.StagingInfo{}, err
	}
	return stagingInfo, nil
}

func requestCompanyProfile(ctx backoff.BackOffContext, symbol api.Symbol) (api.CompanyProfileResponse, error) {
	context := provideContext(ctx)
	cmdAppSecrets, err := provideAppSecrets()
	if err != nil {
		return api.CompanyProfileResponse{}, err
	}
	cmdApiAuthContext := provideApiAuthContext(context, cmdAppSecrets)
	defaultApiService := provideApiServiceClient()
	ticker := _wireTickerValue
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	companyProfileRequest := buildCompanyProfileRequest(symbol)
	companyProfileResponse, err := requestCompanyProfileImpl(cmdApiAuthContext, defaultApiService, ticker, backOff, notify, companyProfileRequest)
	if err != nil {
		return api.CompanyProfileResponse{}, err
	}
	return companyProfileResponse, nil
}

func saveCompanyProfile(ctx backoff.BackOffContext, jobRunId uint64, pool2 *pgxpool.Pool, cp api.CompanyProfileResponse) error {
	context := provideContext(ctx)
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	error2 := db2.SaveCompanyProfile(context, jobRunId, pool2, backOff, notify, cp)
	return error2
}

func stageCompanyProfiles(ctx backoff.BackOffContext, jobRunId uint64, pool2 *pgxpool.Pool) (db2.StagingInfo, error) {
	context := provideContext(ctx)
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	stagingInfo, err := db2.StageCompanyProfiles(context, jobRunId, pool2, backOff, notify)
	if err != nil {
		return db2.StagingInfo{}, err
	}
	return stagingInfo, nil
}

func queryMostRecentCandles(ctx backoff.BackOffContext, jobRunId uint64, pool2 *pgxpool.Pool) (db2.LatestCandles, error) {
	context := provideContext(ctx)
	backOff := provideBackOff(ctx)
	notify := backoffNotifier(context)
	latestCandles, err := db2.LookupLatestCandles(context, pool2, backOff, notify)
	if err != nil {
		return nil, err
	}
	return latestCandles, nil
}

func pool(ctx context.Context) (*pgxpool.Pool, func(), error) {
	userinfo, err := provideDbSecrets()
	if err != nil {
		return nil, nil, err
	}
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return nil, nil, err
	}
	url, err := provideDataSourceName(userinfo, cmdAppConfig)
	if err != nil {
		return nil, nil, err
	}
	cmdDbConnPoolConfig := cmdAppConfig.DbConnPoolConfig
	cmdDbPoolDsn, err := provideDbPoolDsn(url, cmdDbConnPoolConfig)
	if err != nil {
		return nil, nil, err
	}
	pgxpoolPool, cleanup, err := provideDbConnPool(ctx, cmdDbPoolDsn)
	if err != nil {
		return nil, nil, err
	}
	return pgxpoolPool, func() {
		cleanup()
	}, nil
}

func logger() (gke.Logger, func()) {
	gkeLogger, cleanup := provideLogger()
	return gkeLogger, func() {
		cleanup()
	}
}

func migrator(lg gke.Logger) (*migrate.Migrate, error) {
	userinfo, err := provideDbSecrets()
	if err != nil {
		return nil, err
	}
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return nil, err
	}
	url, err := provideDataSourceName(userinfo, cmdAppConfig)
	if err != nil {
		return nil, err
	}
	migrationSourceURL := cmdAppConfig.MigrationSourceURL
	migrateMigrate, err := provideMigrator(lg, url, migrationSourceURL)
	if err != nil {
		return nil, err
	}
	return migrateMigrate, nil
}

// wire.go:

var (
	cfg    = wire.NewSet(provideAppConfig, provideAppSecrets, provideTimezone, wire.FieldsOf(new(*appConfig), "MigrationSourceURL"))
	client = wire.NewSet(provideApiServiceClient, provideApiAuthContext, buildCandleRequest, buildStocksRequest, buildCompanyProfileRequest, wire.FieldsOf(new(*appConfig), "Exchange", "Resolution"), wire.Value(clientTicker))
	db     = wire.NewSet(provideDataSourceName, provideDbSecrets, provideDbConnPool, wire.FieldsOf(new(*appConfig), "DbConnPoolConfig"), provideDbPoolDsn)
	bo     = wire.NewSet(provideBackOff, provideContext, backoffNotifier)
)

var clientTicker = time.NewTicker(time.Second)

func provideBackOff(bo2 backoff.BackOffContext) backoff.BackOff {
	return bo2
}

func provideContext(bo2 backoff.BackOffContext) context.Context {
	return bo2.Context()
}

// Code generated by Wire. DO NOT EDIT.

//go:generate wire
//+build !wireinject

package cmd

import (
	"context"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/gke"
	"github.com/ajjensen13/stocker/internal/extract"
	"github.com/ajjensen13/stocker/internal/load"
	"github.com/ajjensen13/stocker/internal/model"
	"github.com/ajjensen13/stocker/internal/stage"
	"github.com/ajjensen13/stocker/internal/transform"
	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"net/url"
	"time"
)

import (
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// Injectors from wire.go:

func timezone() (*time.Location, error) {
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return nil, err
	}
	location, err := provideTimezone(cmdAppConfig)
	if err != nil {
		return nil, err
	}
	return location, nil
}

func extractStocks(ctx context.Context, lg gke.Logger) ([]finnhub.Stock, error) {
	cmdAppSecrets, err := provideAppSecrets()
	if err != nil {
		return nil, err
	}
	cmdApiAuthContext := provideApiAuthContext(ctx, cmdAppSecrets)
	defaultApiService := provideApiServiceClient()
	backOff := provideBackoffMedium()
	notify := provideBackoffNotifier(lg)
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return nil, err
	}
	v, err := provideStocks(cmdApiAuthContext, lg, defaultApiService, backOff, notify, cmdAppConfig)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func loadStocks(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, ss []finnhub.Stock) error {
	backOff := provideBackoffMedium()
	notify := provideBackoffNotifier(lg)
	error2 := load.Stocks(ctx, lg, pool, jobRunId, ss, backOff, notify)
	return error2
}

func stageStocks(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (stage.StagingInfo, error) {
	backOff := provideBackoffMedium()
	notify := provideBackoffNotifier(lg)
	stagingInfo, err := stage.Stocks(ctx, lg, pool, jobRunId, backOff, notify)
	if err != nil {
		return stage.StagingInfo{}, err
	}
	return stagingInfo, nil
}

func extractCandles(ctx context.Context, lg gke.Logger, stock finnhub.Stock, latest latestStocks) (extract.StockCandlesWithMetadata, error) {
	cmdAppSecrets, err := provideAppSecrets()
	if err != nil {
		return extract.StockCandlesWithMetadata{}, err
	}
	cmdApiAuthContext := provideApiAuthContext(ctx, cmdAppSecrets)
	defaultApiService := provideApiServiceClient()
	backOff := provideBackoffShort()
	notify := provideBackoffNotifier(lg)
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return extract.StockCandlesWithMetadata{}, err
	}
	cmdLatestStock := provideLatestStock(stock, latest)
	location, err := provideTimezone(cmdAppConfig)
	if err != nil {
		return extract.StockCandlesWithMetadata{}, err
	}
	cmdCandleConfig := provideCandleConfig(cmdAppConfig, cmdLatestStock, location)
	stockCandlesWithMetadata, err := provideCandles(cmdApiAuthContext, lg, defaultApiService, backOff, notify, stock, cmdCandleConfig)
	if err != nil {
		return extract.StockCandlesWithMetadata{}, err
	}
	return stockCandlesWithMetadata, nil
}

func transformCandles(es finnhub.Stock, symbol string, ec finnhub.StockCandles) ([]model.Candle, error) {
	location, err := timezone()
	if err != nil {
		return nil, err
	}
	v, err := transform.Candles(symbol, ec, location)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func loadCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, ss extract.StockCandlesWithMetadata) error {
	backOff := provideBackoffMedium()
	notify := provideBackoffNotifier(lg)
	error2 := load.Candles(ctx, jobRunId, pool, ss, backOff, notify)
	return error2
}

func stageCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, symbol string) (stage.StagingInfo, error) {
	backOff := provideBackoffMedium()
	notify := provideBackoffNotifier(lg)
	location, err := timezone()
	if err != nil {
		return stage.StagingInfo{}, err
	}
	stagingInfo, err := stage.Candles(ctx, jobRunId, pool, backOff, notify, location)
	if err != nil {
		return stage.StagingInfo{}, err
	}
	return stagingInfo, nil
}

func stage52WkCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, symbol string) (stage.StagingInfo, error) {
	backOff := provideBackoffLong()
	notify := provideBackoffNotifier(lg)
	stagingInfo, err := stage.Candles52Wk(ctx, lg, jobRunId, pool, symbol, backOff, notify)
	if err != nil {
		return stage.StagingInfo{}, err
	}
	return stagingInfo, nil
}

func extractCompanyProfile(ctx context.Context, lg gke.Logger, stock finnhub.Stock) (finnhub.CompanyProfile2, error) {
	cmdAppSecrets, err := provideAppSecrets()
	if err != nil {
		return finnhub.CompanyProfile2{}, err
	}
	cmdApiAuthContext := provideApiAuthContext(ctx, cmdAppSecrets)
	defaultApiService := provideApiServiceClient()
	backOff := provideBackoffShort()
	notify := provideBackoffNotifier(lg)
	companyProfile2, err := provideCompanyProfiles(cmdApiAuthContext, lg, defaultApiService, backOff, notify, stock)
	if err != nil {
		return finnhub.CompanyProfile2{}, err
	}
	return companyProfile2, nil
}

func transformCompanyProfile(ecp finnhub.CompanyProfile2) (model.CompanyProfile, error) {
	companyProfile, err := transform.CompanyProfile(ecp)
	if err != nil {
		return model.CompanyProfile{}, err
	}
	return companyProfile, nil
}

func loadCompanyProfile(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, cp finnhub.CompanyProfile2) error {
	backOff := provideBackoffShort()
	notify := provideBackoffNotifier(lg)
	error2 := load.CompanyProfile(ctx, jobRunId, pool, cp, backOff, notify)
	return error2
}

func stageCompanyProfiles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (stage.StagingInfo, error) {
	backOff := provideBackoffMedium()
	notify := provideBackoffNotifier(lg)
	stagingInfo, err := stage.CompanyProfiles(ctx, jobRunId, pool, backOff, notify)
	if err != nil {
		return stage.StagingInfo{}, err
	}
	return stagingInfo, nil
}

func queryMostRecentCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (latestStocks, error) {
	backOff := provideBackoffMedium()
	notify := provideBackoffNotifier(lg)
	v, err := extract.LatestCandles(ctx, pool, backOff, notify)
	if err != nil {
		return nil, err
	}
	cmdLatestStocks := provideLatestStocks(v)
	return cmdLatestStocks, nil
}

func dataSourceName() (*url.URL, error) {
	userinfo, err := provideDbSecrets()
	if err != nil {
		return nil, err
	}
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return nil, err
	}
	urlURL, err := provideDataSourceName(userinfo, cmdAppConfig)
	if err != nil {
		return nil, err
	}
	return urlURL, nil
}

func openPool(ctx context.Context, lg gke.Logger) (*pgxpool.Pool, func(), error) {
	userinfo, err := provideDbSecrets()
	if err != nil {
		return nil, nil, err
	}
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return nil, nil, err
	}
	urlURL, err := provideDataSourceName(userinfo, cmdAppConfig)
	if err != nil {
		return nil, nil, err
	}
	cmdDbConnPoolConfig := cmdAppConfig.DbConnPoolConfig
	cmdDbPoolDsn, err := provideDbPoolDsn(urlURL, cmdDbConnPoolConfig)
	if err != nil {
		return nil, nil, err
	}
	pool, cleanup, err := provideDbConnPool(ctx, lg, cmdDbPoolDsn)
	if err != nil {
		return nil, nil, err
	}
	return pool, func() {
		cleanup()
	}, nil
}

func migrationSourceURL() (string, error) {
	cmdAppConfig, err := provideAppConfig()
	if err != nil {
		return "", err
	}
	string2 := provideMigrationSourceURL(cmdAppConfig)
	return string2, nil
}

func logger() (gke.Logger, func()) {
	gkeLogger, cleanup := provideLogger()
	return gkeLogger, func() {
		cleanup()
	}
}

func migrator(lg gke.Logger) (*migrate.Migrate, error) {
	urlURL, err := dataSourceName()
	if err != nil {
		return nil, err
	}
	string2, err := migrationSourceURL()
	if err != nil {
		return nil, err
	}
	migrateMigrate, err := provideMigrator(lg, urlURL, string2)
	if err != nil {
		return nil, err
	}
	return migrateMigrate, nil
}

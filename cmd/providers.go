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
	"errors"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/config"
	"github.com/ajjensen13/gke"
	db2 "github.com/ajjensen13/stocker/internal/db"
	"github.com/ajjensen13/stocker/internal/util"
	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"net/url"
	"sync"
	"time"

	"github.com/ajjensen13/stocker/internal/api"
)

func provideTimezone(appConfig *appConfig) (*time.Location, error) {
	if appConfig.Timezone == "" {
		return time.UTC, nil
	}
	return time.LoadLocation(string(appConfig.Timezone))
}

var (
	pkgAppSecrets     appSecrets
	pkgAppSecretsErr  error
	pkgAppSecretsOnce sync.Once
)

func provideAppSecrets() (*appSecrets, error) {
	pkgAppSecretsOnce.Do(func() {
		pkgAppSecretsErr = config.InterfaceJson(apiSecretName, &pkgAppSecrets)
	})
	if pkgAppSecretsErr != nil {
		return nil, pkgAppSecretsErr
	}
	return &pkgAppSecrets, nil
}

func provideApiServiceClient() *finnhub.DefaultApiService {
	return finnhub.NewAPIClient(finnhub.NewConfiguration()).DefaultApi
}

func provideApiAuthContext(ctx context.Context, secrets *appSecrets) apiAuthContext {
	return context.WithValue(ctx, finnhub.ContextAPIKey, finnhub.APIKey{Key: secrets.ApiKey})
}

var (
	pkgAppConfig     appConfig
	pkgAppConfigErr  error
	pkgAppConfigOnce sync.Once
)

func provideAppConfig() (*appConfig, error) {
	pkgAppConfigOnce.Do(func() {
		pkgAppConfigErr = config.InterfaceJson(appConfigName, &pkgAppConfig)
	})
	if pkgAppConfigErr != nil {
		return nil, pkgAppConfigErr
	}
	return &pkgAppConfig, nil
}

var (
	pkgDbSecrets     *url.Userinfo
	pkgDbSecretsErr  error
	pkgDbSecretsOnce sync.Once
)

func provideDbSecrets() (*url.Userinfo, error) {
	pkgDbSecretsOnce.Do(func() {
		pkgDbSecrets, pkgDbSecretsErr = config.Userinfo(dbSecretName)
	})
	if pkgDbSecretsErr != nil {
		return nil, pkgDbSecretsErr
	}
	return pkgDbSecrets, nil
}

type MaxElapsedTime time.Duration

func backoffNotifier(ctx context.Context) backoff.Notify {
	return func(err error, duration time.Duration) {
		if errors.Is(err, api.ErrToManyRequests) {
			util.Logf(ctx, logging.Debug, "request exceeded rate limit, waiting %v before retrying: %v", duration, err)
			return
		}
		util.Logf(ctx, logging.Warning, "request failed, waiting %v before retrying: %v", duration, err)
	}
}

func latestCandleTimeFromLatestCandles(symbol api.Symbol, latestCandles db2.LatestCandles) db2.LatestCandleTime {
	return latestCandles[symbol]
}

func buildStocksRequest(exchange Exchange) api.StocksRequest {
	return api.StocksRequest{Exchange: api.Exchange(exchange)}
}

func buildCandleRequest(cfg *appConfig, lct db2.LatestCandleTime, tz *time.Location, symbol api.Symbol) api.CandlesRequest {
	var endDate time.Time
	if cfg.EndDate.IsZero() {
		now := time.Now().In(tz)
		endDate = time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, tz)
	} else {
		endDate = cfg.EndDate.In(tz)
	}

	var startDate time.Time
	if cfg.StartDate.IsZero() {
		startDate = time.Time(lct).Add(time.Second)
	} else {
		startDate = cfg.StartDate.In(tz)
	}

	if endDate.Before(startDate) {
		endDate, startDate = startDate, endDate
	}

	return api.CandlesRequest{
		Symbol:     symbol,
		Resolution: api.Resolution(cfg.Resolution),
		From:       api.From(startDate),
		To:         api.To(endDate),
	}
}

func buildCompanyProfileRequest(symbol api.Symbol) api.CompanyProfileRequest {
	return api.CompanyProfileRequest{Symbol: symbol}
}

func requestCandlesImpl(ctx apiAuthContext, client *finnhub.DefaultApiService, throttler *time.Ticker, bo backoff.BackOff, bon backoff.Notify, req api.CandlesRequest) (api.CandlesResponse, error) {
	ctx = util.WithLoggerValue(ctx, "action", "request")
	util.Logf(ctx, logging.Debug, "requesting %q candles from finnhub. (%v — %v) / %s", req.Symbol, req.From, req.To, req.Resolution)
	return api.RequestCandles(ctx, client, throttler, bo, bon, req)
}

func requestStocksImpl(ctx apiAuthContext, client *finnhub.DefaultApiService, throttler *time.Ticker, bo backoff.BackOff, bon backoff.Notify, req api.StocksRequest) (api.StocksResponse, error) {
	ctx = util.WithLoggerValue(ctx, "action", "request")
	util.Logf(ctx, logging.Debug, "requesting %q stocks from finnhub", req.Exchange)
	return api.RequestStocks(ctx, client, throttler, bo, bon, req)
}

func requestCompanyProfileImpl(ctx apiAuthContext, client *finnhub.DefaultApiService, throttler *time.Ticker, bo backoff.BackOff, bon backoff.Notify, req api.CompanyProfileRequest) (api.CompanyProfileResponse, error) {
	ctx = util.WithLoggerValue(ctx, "action", "request")
	util.Logf(ctx, logging.Debug, "requesting %q company profiles from finnhub", req.Symbol)
	return api.RequestCompanyProfile(ctx, client, throttler, bo, bon, req)
}

func provideDataSourceName(user *url.Userinfo, cfg *appConfig) (dsn *url.URL, err error) {
	dsn, err = url.Parse(string(cfg.DataSourceName))
	if err != nil {
		return nil, fmt.Errorf("failed to parse data source name: %w", err)
	}
	dsn.User = user

	return dsn, nil
}

type dbPoolDsn *url.URL

func provideDbConnPool(ctx context.Context, dsn dbPoolDsn) (ret *pgxpool.Pool, cleanup func(), err error) {
	u := (*url.URL)(dsn)
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to parse pgxpool config: %w", err)
	}
	cfg.ConnConfig.Logger = pgxLoggerAdapter{}
	cfg.ConnConfig.LogLevel = pgx.LogLevelWarn

	pool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to open database connection pool: %w", err)
	}

	util.Logf(util.WithLoggerValue(ctx, "pg_conn_pool_info", struct {
		MaxConnLifetime   string
		MaxConnIdleTime   string
		MaxConns          int32
		MinConns          int32
		HealthCheckPeriod string
		LazyConnect       bool
		Host              string
		Port              uint16
		Database          string
		User              string
		ConnectTimeout    string
		LogLevel          string
	}{
		MaxConnLifetime:   cfg.MaxConnLifetime.String(),
		MaxConnIdleTime:   cfg.MaxConnIdleTime.String(),
		MaxConns:          cfg.MaxConns,
		MinConns:          cfg.MinConns,
		HealthCheckPeriod: cfg.HealthCheckPeriod.String(),
		LazyConnect:       cfg.LazyConnect,
		Host:              cfg.ConnConfig.Host,
		Port:              cfg.ConnConfig.Port,
		Database:          cfg.ConnConfig.Database,
		User:              cfg.ConnConfig.User,
		// _: 			   cfg.ConnConfig.Password, DON'T INCLUDE THIS
		ConnectTimeout: cfg.ConnConfig.ConnectTimeout.String(),
		LogLevel:       cfg.ConnConfig.LogLevel.String(),
	}), logging.Debug, "database connection pool created")

	return pool, func() {
		pool.Close()
		util.Logf(ctx, logging.Debug, "database connection pool closed")
	}, nil
}

func provideDbPoolDsn(dsn *url.URL, poolCfg dbConnPoolConfig) (dbPoolDsn, error) {
	poolDsn, err := url.Parse(dsn.String())
	if err != nil {
		return nil, err
	}

	q := poolDsn.Query()
	if poolCfg.MaxConnLifetime != "" {
		q.Add("pool_max_conn_lifetime", poolCfg.MaxConnLifetime)
	}

	if poolCfg.MaxConnIdleTime != "" {
		q.Add("pool_max_conn_idle_time", poolCfg.MaxConnIdleTime)
	}

	if poolCfg.HealthCheckPeriod != "" {
		q.Add("pool_health_check_period", poolCfg.HealthCheckPeriod)
	}

	if poolCfg.MinConns > 0 {
		q.Add("pool_min_conns", fmt.Sprintf("%d", poolCfg.MinConns))
	}

	if poolCfg.MaxConns > 0 {
		q.Add("pool_max_conns", fmt.Sprintf("%d", poolCfg.MaxConns))
	}

	poolDsn.RawQuery = q.Encode()
	return poolDsn, nil
}

func provideLogger() (lg gke.Logger, cleanup func()) {
	lg, cleanup, err := gke.NewLogger(context.Background())
	if err != nil {
		panic(err)
	}

	gke.LogEnv(lg)
	gke.LogMetadata(lg)
	gke.LogGoRuntime(lg)

	return lg, cleanup
}

func provideMigrator(lg gke.Logger, databaseURL *url.URL, sourceURL MigrationSourceURL) (m *migrate.Migrate, err error) {
	m, err = migrate.New(string(sourceURL), databaseURL.String())
	if err != nil {
		return nil, err
	}
	m.Log = migrationLogger{lg}
	return m, err
}

type migrationLogger struct {
	gke.Logger
}

func (m migrationLogger) Printf(format string, v ...interface{}) {
	m.Defaultf(format, v...)
}

func (m migrationLogger) Verbose() bool {
	return false
}

type pgxLoggerAdapter struct{}

func (p pgxLoggerAdapter) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	var severity logging.Severity
	switch level {
	case pgx.LogLevelError:
		severity = logging.Error
	case pgx.LogLevelWarn:
		severity = logging.Warning
	case pgx.LogLevelInfo:
		severity = logging.Info
	case pgx.LogLevelDebug:
		severity = logging.Debug
	case pgx.LogLevelTrace, pgx.LogLevelNone:
		severity = logging.Default
	default:
		panic(fmt.Sprintf("unknown log level: %v", level))
	}

	util.Logf(util.WithLoggerValue(ctx, "pgx_data", data), severity, msg)
}

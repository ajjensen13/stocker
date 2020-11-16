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
	"errors"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/config"
	"github.com/ajjensen13/gke"
	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"net/url"
	"sync"
	"time"

	"github.com/ajjensen13/stocker/internal/extract"
	"github.com/ajjensen13/stocker/internal/util"
)

func provideTimezone(appConfig *appConfig) (*time.Location, error) {
	if appConfig.Timezone == "" {
		return time.UTC, nil
	}
	return time.LoadLocation(appConfig.Timezone)
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

func provideBackoffShort() backoff.BackOff {
	result := backoff.NewExponentialBackOff()
	result.InitialInterval = time.Second
	result.MaxElapsedTime = util.ShortReqTimeout * 5
	return result
}

func provideBackoffMedium() backoff.BackOff {
	result := backoff.NewExponentialBackOff()
	result.InitialInterval = time.Second
	result.MaxElapsedTime = util.MedReqTimeout * 5
	return result
}

func provideBackoffLong() backoff.BackOff {
	result := backoff.NewExponentialBackOff()
	result.InitialInterval = time.Minute
	result.MaxElapsedTime = util.LongReqTimeout * 5
	return result
}

func provideBackoffNotifier(lg gke.Logger) backoff.Notify {
	return func(err error, duration time.Duration) {
		if errors.Is(err, extract.ErrToManyRequests) {
			lg.Info(gke.NewFmtMsgData("request exceeded rate limit, waiting %v before retrying: %v", duration, err))
			return
		}
		lg.Warning(gke.NewFmtMsgData("request failed, waiting %v before retrying: %v", duration, err))
	}
}

func provideLatestStock(stock finnhub.Stock, latest latestStocks) latestStock {
	return latestStock{
		symbol:    stock.Symbol,
		timestamp: latest[stock.Symbol],
	}
}

func provideCandleConfig(cfg *appConfig, latest latestStock, tz *time.Location) candleConfig {
	var endDate time.Time
	if cfg.EndDate.IsZero() {
		now := time.Now().In(tz)
		endDate = time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, tz)
	} else {
		endDate = cfg.EndDate.In(tz)
	}

	var startDate time.Time
	if cfg.StartDate.IsZero() {
		startDate = latest.timestamp.Add(time.Second)
	} else {
		startDate = cfg.StartDate.In(tz)
	}

	if endDate.Before(startDate) {
		endDate, startDate = startDate, endDate
	}

	return candleConfig{
		resolution: cfg.Resolution,
		startDate:  startDate,
		endDate:    endDate,
	}
}

func provideCandles(ctx apiAuthContext, lg gke.Logger, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, s finnhub.Stock, cfg candleConfig) (finnhub.StockCandles, error) {
	lg.Default(gke.NewMsgData(fmt.Sprintf("requesting %q candles from finnhub. (%v — %v) / %s", s.Symbol, cfg.startDate, cfg.endDate, cfg.resolution),
		struct {
			Symbol             string
			StartDate, EndDate time.Time
			Resolution         string
		}{s.Symbol, cfg.startDate, cfg.endDate, cfg.resolution}))
	return extract.Candles(ctx, client, bo, bon, s, cfg.resolution, cfg.startDate, cfg.endDate)
}

func provideStocks(ctx apiAuthContext, lg gke.Logger, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, cfg *appConfig) ([]finnhub.Stock, error) {
	lg.Defaultf("requesting %q stocks from finnhub", cfg.Exchange)
	return extract.Stocks(ctx, client, bo, bon, cfg.Exchange)
}

func provideCompanyProfiles(ctx apiAuthContext, lg gke.Logger, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, stock finnhub.Stock) (finnhub.CompanyProfile2, error) {
	lg.Defaultf("requesting %q company profiles from finnhub", stock.Symbol)
	return extract.CompanyProfile(ctx, client, bo, bon, stock)
}

func provideLatestStocks(latest map[string]time.Time) latestStocks {
	return latestStocks(latest)
}

func provideDataSourceName(user *url.Userinfo, cfg *appConfig) (dsn *url.URL, err error) {
	dsn, err = url.Parse(cfg.DataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse data source name: %w", err)
	}
	dsn.User = user

	return dsn, nil
}

type dbPoolDsn string

func provideDbConnPool(ctx context.Context, dsn dbPoolDsn) (ret *pgxpool.Pool, cleanup func(), err error) {
	pool, err := pgxpool.Connect(ctx, string(dsn))
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to open database connection pool: %w", err)
	}

	return pool, pool.Close, nil
}

func provideDbPoolDsn(dsn *url.URL, poolCfg dbConnPoolConfig) (dbPoolDsn, error) {
	poolDsn, err := url.Parse(dsn.String())
	if err != nil {
		return "", err
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
	return dbPoolDsn(poolDsn.String()), nil
}

func provideDbTx(ctx context.Context, conn *pgxpool.Pool, opts pgx.TxOptions) (pgx.Tx, error) {
	return conn.BeginTx(ctx, opts)
}

func provideMigrationSourceURL(cfg *appConfig) string {
	return cfg.MigrationSourceURL
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

func provideMigrator(lg gke.Logger, databaseURL *url.URL, sourceURL string) (m *migrate.Migrate, err error) {
	m, err = migrate.New(sourceURL, databaseURL.String())
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

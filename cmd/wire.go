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
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/gke"
	"github.com/golang-migrate/migrate/v4"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
	"net/url"
	"time"

	"github.com/ajjensen13/stocker/internal/extract"
	"github.com/ajjensen13/stocker/internal/load"
	"github.com/ajjensen13/stocker/internal/model"
	"github.com/ajjensen13/stocker/internal/stage"
	"github.com/ajjensen13/stocker/internal/transform"
)

func timezone() (tz *time.Location, err error) {
	panic(wire.Build(provideTimezone, provideAppConfig))
}

func extractStocks(ctx context.Context, lg gke.Logger) (ss []finnhub.Stock, err error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideAppConfig, provideBackoffMedium, provideBackoffNotifier, provideApiAuthContext, provideStocks))
}

func loadStocks(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, ss []finnhub.Stock) (err error) {
	panic(wire.Build(load.Stocks, provideBackoffMedium, provideBackoffNotifier))
}

func stageStocks(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (si stage.StagingInfo, err error) {
	panic(wire.Build(stage.Stocks, provideBackoffMedium, provideBackoffNotifier))
}

func extractCandles(ctx context.Context, lg gke.Logger, stock finnhub.Stock, latest latestStocks) (sc extract.StockCandlesWithMetadata, err error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideAppConfig, provideBackoffShort, provideBackoffNotifier, provideApiAuthContext, provideCandles, provideCandleConfig, provideLatestStock, provideTimezone))
}

func transformCandles(es finnhub.Stock, symbol string, ec finnhub.StockCandles) ([]model.Candle, error) {
	panic(wire.Build(transform.Candles, timezone))
}

func loadCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, ss extract.StockCandlesWithMetadata) (err error) {
	panic(wire.Build(load.Candles, provideBackoffMedium, provideBackoffNotifier))
}

func stageCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, symbol string) (si stage.StagingInfo, err error) {
	panic(wire.Build(stage.Candles, provideBackoffMedium, provideBackoffNotifier, timezone))
}

func stage52WkCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, symbol string) (si stage.StagingInfo, err error) {
	panic(wire.Build(stage.Candles52Wk, provideBackoffLong, provideBackoffNotifier))
}

func extractCompanyProfile(ctx context.Context, lg gke.Logger, stock finnhub.Stock) (cp finnhub.CompanyProfile2, err error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideBackoffShort, provideBackoffNotifier, provideApiAuthContext, provideCompanyProfiles))
}

func transformCompanyProfile(ecp finnhub.CompanyProfile2) (model.CompanyProfile, error) {
	panic(wire.Build(transform.CompanyProfile))
}

func loadCompanyProfile(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, cp finnhub.CompanyProfile2) (err error) {
	panic(wire.Build(load.CompanyProfile, provideBackoffShort, provideBackoffNotifier))
}

func stageCompanyProfiles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (si stage.StagingInfo, err error) {
	panic(wire.Build(stage.CompanyProfiles, provideBackoffMedium, provideBackoffNotifier))
}

func queryMostRecentCandles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (ls latestStocks, err error) {
	panic(wire.Build(extract.LatestCandles, provideLatestStocks, provideBackoffMedium, provideBackoffNotifier))
}

func dataSourceName() (dsn *url.URL, err error) {
	panic(wire.Build(provideDataSourceName, provideDbSecrets, provideAppConfig))
}

func openPool(ctx context.Context, lg gke.Logger) (*pgxpool.Pool, func(), error) {
	panic(wire.Build(provideDbConnPool, provideDataSourceName, provideAppConfig, provideDbSecrets, wire.FieldsOf(new(*appConfig), "DbConnPoolConfig"), provideDbPoolDsn))
}

func migrationSourceURL() (uri string, err error) {
	panic(wire.Build(provideMigrationSourceURL, provideAppConfig))
}

func logger() (lg gke.Logger, cleanup func()) {
	panic(wire.Build(provideLogger))
}

func migrator(lg gke.Logger) (m *migrate.Migrate, err error) {
	panic(wire.Build(provideMigrator, migrationSourceURL, dataSourceName))
}

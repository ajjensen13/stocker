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
	"github.com/ajjensen13/stocker/internal/extract"
	"github.com/ajjensen13/stocker/internal/load"
	"github.com/ajjensen13/stocker/internal/stage"
	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	cfg    = wire.NewSet(provideAppConfig, provideAppSecrets, provideCandleConfig, provideTimezone, wire.FieldsOf(new(*appConfig), "MigrationSourceURL"))
	client = wire.NewSet(provideApiServiceClient, provideApiAuthContext)
	db     = wire.NewSet(provideDataSourceName, provideDbSecrets, provideDbConnPool, wire.FieldsOf(new(*appConfig), "DbConnPoolConfig"), provideDbPoolDsn)
	bo     = wire.NewSet(provideBackOff, provideContext, provideBackoffNotifier)
)

func provideBackOff(bo backoff.BackOffContext) backoff.BackOff {
	return bo
}

func provideContext(bo backoff.BackOffContext) context.Context {
	return bo.Context()
}

func extractStocks(ctx backoff.BackOffContext, lg gke.Logger) (ss []finnhub.Stock, err error) {
	panic(wire.Build(cfg, client, bo, provideStocks))
}

func loadStocks(ctx backoff.BackOffContext, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, ss []finnhub.Stock) (err error) {
	panic(wire.Build(bo, load.Stocks))
}

func stageStocks(ctx backoff.BackOffContext, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (si stage.StagingInfo, err error) {
	panic(wire.Build(bo, stage.Stocks))
}

func extractCandles(ctx backoff.BackOffContext, lg gke.Logger, stock finnhub.Stock, latest latestStocks) (sc extract.StockCandlesWithMetadata, err error) {
	panic(wire.Build(cfg, client, bo, provideCandles, provideLatestStock))
}

func loadCandles(ctx backoff.BackOffContext, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, ss extract.StockCandlesWithMetadata) (err error) {
	panic(wire.Build(bo, load.Candles))
}

func stageCandles(ctx backoff.BackOffContext, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, symbol string) (si stage.StagingInfo, err error) {
	panic(wire.Build(cfg, bo, stage.Candles))
}

func stage52WkCandles(ctx backoff.BackOffContext, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, symbol string) (si stage.StagingInfo, err error) {
	panic(wire.Build(bo, stage.Candles52Wk))
}

func extractCompanyProfile(ctx backoff.BackOffContext, lg gke.Logger, stock finnhub.Stock) (cp finnhub.CompanyProfile2, err error) {
	panic(wire.Build(cfg, client, bo, provideCompanyProfiles))
}

func loadCompanyProfile(ctx backoff.BackOffContext, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, cp finnhub.CompanyProfile2) (err error) {
	panic(wire.Build(bo, load.CompanyProfile))
}

func stageCompanyProfiles(ctx backoff.BackOffContext, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (si stage.StagingInfo, err error) {
	panic(wire.Build(bo, stage.CompanyProfiles))
}

func queryMostRecentCandles(ctx backoff.BackOffContext, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool) (ls latestStocks, err error) {
	panic(wire.Build(bo, extract.LatestCandles, provideLatestStocks))
}

func pool(ctx context.Context, lg gke.Logger) (*pgxpool.Pool, func(), error) {
	panic(wire.Build(cfg, db))
}

func logger() (lg gke.Logger, cleanup func()) {
	panic(wire.Build(provideLogger))
}

func migrator(lg gke.Logger) (m *migrate.Migrate, err error) {
	panic(wire.Build(cfg, db, provideMigrator))
}

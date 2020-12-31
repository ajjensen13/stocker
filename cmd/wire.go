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
	"github.com/ajjensen13/gke"
	"github.com/ajjensen13/stocker/internal/api"
	db2 "github.com/ajjensen13/stocker/internal/db"
	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
)

var (
	cfg    = wire.NewSet(provideAppConfig, provideAppSecrets, provideTimezone, wire.FieldsOf(new(*appConfig), "MigrationSourceURL"))
	client = wire.NewSet(provideApiServiceClient, provideApiAuthContext, buildCandleRequest, buildStocksRequest, buildCompanyProfileRequest, wire.FieldsOf(new(*appConfig), "Exchange", "Resolution"), wire.Value(clientTicker))
	db     = wire.NewSet(provideDataSourceName, provideDbSecrets, provideDbConnPool, wire.FieldsOf(new(*appConfig), "DbConnPoolConfig"), provideDbPoolDsn)
	bo     = wire.NewSet(provideBackOff, provideContext, backoffNotifier)
)

var clientTicker = time.NewTicker(time.Second)

func provideBackOff(bo backoff.BackOffContext) backoff.BackOff {
	return bo
}

func provideContext(bo backoff.BackOffContext) context.Context {
	return bo.Context()
}

func requestStocks(ctx backoff.BackOffContext) (api.StocksResponse, error) {
	panic(wire.Build(cfg, client, bo, requestStocksImpl))
}

func saveStocks(ctx backoff.BackOffContext, jobRunId uint64, pool *pgxpool.Pool, resp api.StocksResponse) error {
	panic(wire.Build(bo, db2.SaveStocks))
}

func stageStocks(ctx backoff.BackOffContext, jobRunId uint64, pool *pgxpool.Pool) (db2.StagingInfo, error) {
	panic(wire.Build(bo, db2.StageStocks))
}

func requestCandles(ctx backoff.BackOffContext, symbol api.Symbol, lc db2.LatestCandles) (api.CandlesResponse, error) {
	panic(wire.Build(cfg, client, bo, requestCandlesImpl, latestCandleTimeFromLatestCandles))
}

func saveCandles(ctx backoff.BackOffContext, jobRunId uint64, pool *pgxpool.Pool, resp api.CandlesResponse) error {
	panic(wire.Build(bo, db2.SaveCandles))
}

func stageCandles(ctx backoff.BackOffContext, jobRunId uint64, pool *pgxpool.Pool, resp api.CandlesResponse) (db2.StagingInfo, error) {
	panic(wire.Build(cfg, bo, db2.StageCandles))
}

func stage52WkCandles(ctx backoff.BackOffContext, jobRunId uint64, pool *pgxpool.Pool, resp api.CandlesResponse) (db2.StagingInfo, error) {
	panic(wire.Build(bo, db2.StageCandles52Wk))
}

func requestCompanyProfile(ctx backoff.BackOffContext, symbol api.Symbol) (api.CompanyProfileResponse, error) {
	panic(wire.Build(cfg, client, bo, requestCompanyProfileImpl))
}

func saveCompanyProfile(ctx backoff.BackOffContext, jobRunId uint64, pool *pgxpool.Pool, cp api.CompanyProfileResponse) error {
	panic(wire.Build(bo, db2.SaveCompanyProfile))
}

func stageCompanyProfiles(ctx backoff.BackOffContext, jobRunId uint64, pool *pgxpool.Pool) (db2.StagingInfo, error) {
	panic(wire.Build(bo, db2.StageCompanyProfiles))
}

func queryMostRecentCandles(ctx backoff.BackOffContext, jobRunId uint64, pool *pgxpool.Pool) (db2.LatestCandles, error) {
	panic(wire.Build(bo, db2.LookupLatestCandles))
}

func pool(ctx context.Context) (*pgxpool.Pool, func(), error) {
	panic(wire.Build(cfg, db))
}

func logger() (lg gke.Logger, cleanup func()) {
	panic(wire.Build(provideLogger))
}

func migrator(lg gke.Logger) (*migrate.Migrate, error) {
	panic(wire.Build(cfg, db, provideMigrator))
}

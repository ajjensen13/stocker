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
	"github.com/jackc/pgx/v4"
	"net/url"
	"time"

	"github.com/ajjensen13/stocker/internal/extract"
)

func timezone() (tz *time.Location, err error) {
	panic(wire.Build(provideTimezone, provideAppConfig))
}

func requestStocks(ctx context.Context, lg gke.Logger) (ss []finnhub.Stock, err error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideAppConfig, provideBackoff, provideBackoffNotifier, provideApiAuthContext, provideStocks))
}

func requestCandles(ctx context.Context, lg gke.Logger, stock finnhub.Stock, latest latestStocks) (sc finnhub.StockCandles, err error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideAppConfig, provideBackoff, provideBackoffNotifier, provideApiAuthContext, provideCandles, provideCandleConfig, provideLatestStock, provideTimezone))
}

func requestCompanyProfile(ctx context.Context, lg gke.Logger, stock finnhub.Stock) (cp finnhub.CompanyProfile2, err error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideBackoff, provideBackoffNotifier, provideApiAuthContext, provideCompanyProfiles))
}

func queryMostRecentCandles(ctx context.Context, lg gke.Logger, tx pgx.Tx) (ls latestStocks, err error) {
	panic(wire.Build(extract.LatestStocks, provideLatestStocks))
}

func dataSourceName() (dsn *url.URL, err error) {
	panic(wire.Build(provideDataSourceName, provideDbSecrets, provideAppConfig))
}

func openTx(ctx context.Context) (tx pgx.Tx, cleanup func(), err error) {
	panic(wire.Build(provideDbConnPool, dataSourceName, provideDbConn, provideDbTx, wire.Value(pgx.TxOptions{})))
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

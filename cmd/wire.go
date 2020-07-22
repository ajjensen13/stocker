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
	"github.com/google/wire"
	"github.com/jackc/pgx/v4"
	"time"

	"github.com/ajjensen13/stocker/internal/extract"
)

func timezone() (*time.Location, error) {
	panic(wire.Build(provideTimezone, provideAppConfig))
}

func requestStocks(ctx context.Context, lg gke.Logger) ([]finnhub.Stock, error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideAppConfig, provideBackoff, provideApiAuthContext, provideStocks))
}

func requestCandles(ctx context.Context, lg gke.Logger, stock finnhub.Stock, latest latestStocks) (finnhub.StockCandles, error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideAppConfig, provideBackoff, provideApiAuthContext, provideCandles, provideCandleConfig, provideLatestStock, provideTimezone))
}

func requestCompanyProfile(ctx context.Context, lg gke.Logger, stock finnhub.Stock) (finnhub.CompanyProfile2, error) {
	panic(wire.Build(provideApiServiceClient, provideAppSecrets, provideBackoff, provideApiAuthContext, provideCompanyProfiles))
}

func queryMostRecentCandles(ctx context.Context, lg gke.Logger, tx pgx.Tx) (latestStocks, error) {
	panic(wire.Build(extract.LatestStocks, provideLatestStocks))
}

func openTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, func(), error) {
	panic(wire.Build(provideDbConnPool, provideDbConn, provideDbTx, provideDbSecrets, provideAppConfig))
}

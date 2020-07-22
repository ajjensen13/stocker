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

package extract

import (
	"context"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/antihax/optional"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"time"
)

func Stocks(ctx context.Context, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, exchange string) ([]finnhub.Stock, error) {
	var result []finnhub.Stock
	err := backoff.RetryNotify(func() error {
		ss, _, err := client.StockSymbols(ctx, exchange)
		if err != nil {
			return fmt.Errorf("error while getting stocks: %w", err)
		}
		result = ss
		return nil
	}, bo, bon)

	return result, err
}

func Candles(ctx context.Context, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, stock finnhub.Stock, resolution string, from, to time.Time) (finnhub.StockCandles, error) {
	var result finnhub.StockCandles
	err := backoff.RetryNotify(func() error {
		c, _, err := client.StockCandles(ctx, stock.Symbol, resolution, from.Unix(), to.Unix(), nil)
		if err != nil {
			return fmt.Errorf("error while getting candle for stock %q: %w", stock.Symbol, err)
		}
		result = c
		return nil
	}, bo, bon)

	return result, err
}

func CompanyProfile(ctx context.Context, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, stock finnhub.Stock) (finnhub.CompanyProfile2, error) {
	var result finnhub.CompanyProfile2
	err := backoff.RetryNotify(func() error {
		c, _, err := client.CompanyProfile2(ctx, &finnhub.CompanyProfile2Opts{Symbol: optional.NewString(stock.Symbol)})
		if err != nil {
			return fmt.Errorf("error while getting company profile %q: %w", stock.Symbol, err)
		}
		result = c
		return nil
	}, bo, bon)

	return result, err
}

func LatestStocks(ctx context.Context, tx pgx.Tx) (map[string]time.Time, error) {
	rows, err := tx.Query(ctx, `SELECT DISTINCT "Symbol", MAX("Timestamp") FROM "public"."Candles" GROUP BY "Symbol"`)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest stocks: %w", err)
	}

	result := make(map[string]time.Time)
	for rows.Next() {
		var symbol string
		var timestamp time.Time
		err := rows.Scan(&symbol, &timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to parse latest stocks: %w", err)
		}
		result[symbol] = timestamp
	}

	return result, nil
}

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
	"github.com/cenkalti/backoff/v4"
	"time"
)

func Stocks(ctx context.Context, client *finnhub.DefaultApiService, b backoff.BackOff, exchange string) ([]finnhub.Stock, error) {
	var result []finnhub.Stock
	err := backoff.Retry(func() error {
		ss, _, err := client.StockSymbols(ctx, exchange)
		if err != nil {
			return fmt.Errorf("error while getting stocks: %w", err)
		}
		result = ss
		return nil
	}, b)

	return result, err
}

func Candles(ctx context.Context, client *finnhub.DefaultApiService, b backoff.BackOff, stock finnhub.Stock, resolution string, from, to time.Time) (finnhub.StockCandles, error) {
	var result finnhub.StockCandles
	err := backoff.Retry(func() error {
		c, _, err := client.StockCandles(ctx, stock.Symbol, resolution, from.Unix(), to.Unix(), nil)
		if err != nil {
			return fmt.Errorf("error while getting candle for stock %q: %w", stock.Symbol, err)
		}
		result = c
		return nil
	}, b)

	return result, err
}

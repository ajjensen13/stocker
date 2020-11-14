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
	"errors"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/antihax/optional"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/ajjensen13/stocker/internal/util"
)

func Stocks(ctx context.Context, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, exchange string) ([]finnhub.Stock, error) {
	var result []finnhub.Stock
	err := backoff.RetryNotify(func() error {
		ctx, cancel := context.WithTimeout(ctx, util.MedReqTimeout)
		defer cancel()

		ss, resp, err := client.StockSymbols(ctx, exchange)
		if err != nil {
			return handleErr("error while getting stocks", resp, err)
		}
		result = ss
		return nil
	}, bo, bon)

	return result, err
}

func Candles(ctx context.Context, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, stock finnhub.Stock, resolution string, startDate, endDate time.Time) (finnhub.StockCandles, error) {
	var result finnhub.StockCandles
	err := backoff.RetryNotify(func() error {
		ctx, cancel := context.WithTimeout(ctx, util.ShortReqTimeout)
		defer cancel()

		c, resp, err := client.StockCandles(ctx, stock.Symbol, resolution, startDate.Unix(), endDate.Unix(), nil)
		if err != nil {
			return handleErr(fmt.Sprintf("error while getting candle for stock %q", stock.Symbol), resp, err)
		}
		result = c
		return nil
	}, bo, bon)

	return result, err
}

func CompanyProfile(ctx context.Context, client *finnhub.DefaultApiService, bo backoff.BackOff, bon backoff.Notify, stock finnhub.Stock) (finnhub.CompanyProfile2, error) {
	var result finnhub.CompanyProfile2
	err := backoff.RetryNotify(func() error {
		ctx, cancel := context.WithTimeout(ctx, util.ShortReqTimeout)
		defer cancel()

		c, resp, err := client.CompanyProfile2(ctx, &finnhub.CompanyProfile2Opts{Symbol: optional.NewString(stock.Symbol)})
		if err != nil {
			return handleErr(fmt.Sprintf("error while getting company profile %q", stock.Symbol), resp, err)
		}
		result = c
		return nil
	}, bo, bon)

	return result, err
}

func LatestCandles(ctx context.Context, tx pgx.Tx, bo backoff.BackOff, bon backoff.Notify) (map[string]time.Time, error) {
	var ret map[string]time.Time
	err := backoff.RetryNotify(func() error {
		ctx, cancel := context.WithTimeout(ctx, util.MedReqTimeout)
		defer cancel()

		rows, err := tx.Query(ctx, `SELECT DISTINCT "Symbol", MAX("Timestamp") FROM src."Candles" GROUP BY "Symbol"`)
		if err != nil {
			return fmt.Errorf("failed to query latest stocks: %w", err)
		}

		ret = make(map[string]time.Time)
		for rows.Next() {
			var symbol string
			var timestamp time.Time
			err := rows.Scan(&symbol, &timestamp)
			if err != nil {
				return fmt.Errorf("failed to parse latest stocks: %w", err)
			}
			ret[symbol] = timestamp
		}
		return nil
	}, bo, bon)

	if err != nil {
		return nil, err
	}

	return ret, nil
}

var ErrToManyRequests = errors.New("error: too many requests")

func handleErr(msg string, resp *http.Response, err error) error {
	switch {
	case resp == nil:
		break
	case resp.StatusCode == http.StatusTooManyRequests:
		err = fmt.Errorf("%s: %w", msg, ErrToManyRequests)
	case resp.Body != nil:
		defer resp.Body.Close()
		body, readErr := ioutil.ReadAll(resp.Body)
		if readErr != nil {
			msg = fmt.Sprintf("error while to parsing error response %v. %s", readErr, msg)
			break
		}
		msg = fmt.Sprintf("%s (%s)", msg, body)
	}
	return fmt.Errorf("%s: %w", msg, err)
}

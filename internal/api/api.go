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

package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/antihax/optional"
	"github.com/cenkalti/backoff/v4"
	"io/ioutil"
	"net/http"
	"time"
)

type Exchange string

type StocksRequest struct {
	Exchange
}

type StocksResponse struct {
	Request  StocksRequest
	Response []finnhub.Stock
}

func RequestStocks(ctx context.Context, client *finnhub.DefaultApiService, ticker *time.Ticker, bo backoff.BackOff, bon backoff.Notify, req StocksRequest) (result StocksResponse, err error) {
	err = backoff.RetryNotify(func() error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting stocks request: %w", ctx.Err())
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()

			stocks, httpResp, err := client.StockSymbols(ctx, string(req.Exchange))
			if err != nil {
				return handleErr("error while getting stocks", httpResp, err)
			}
			result = StocksResponse{Request: req, Response: stocks}
			return nil
		}
	}, bo, bon)
	return
}

type Symbol string
type Resolution string
type From time.Time
type To time.Time

type CandlesRequest struct {
	Symbol
	Resolution
	From // Earlier Date
	To   // Later Date
}

type CandlesResponse struct {
	Request  CandlesRequest
	Response finnhub.StockCandles
}

func RequestCandles(ctx context.Context, client *finnhub.DefaultApiService, ticker *time.Ticker, bo backoff.BackOff, bon backoff.Notify, req CandlesRequest) (result CandlesResponse, err error) {
	err = backoff.RetryNotify(func() error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting candles request: %w", ctx.Err())
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			candles, httpResp, err := client.StockCandles(ctx, string(req.Symbol), string(req.Resolution), time.Time(req.From).Unix(), time.Time(req.To).Unix(), nil)
			if err != nil {
				return handleErr(fmt.Sprintf("error while requesting candle for stock %q", req.Symbol), httpResp, err)
			}

			result = CandlesResponse{Request: req, Response: candles}
			return nil
		}
	}, bo, bon)
	return
}

type CompanyProfileRequest struct {
	Symbol
}

type CompanyProfileResponse struct {
	Request  CompanyProfileRequest
	Response finnhub.CompanyProfile2
}

func RequestCompanyProfile(ctx context.Context, client *finnhub.DefaultApiService, ticker *time.Ticker, bo backoff.BackOff, bon backoff.Notify, req CompanyProfileRequest) (result CompanyProfileResponse, err error) {
	err = backoff.RetryNotify(func() error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("aborting company profile request: %w", ctx.Err())
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			profiles, httpResp, err := client.CompanyProfile2(ctx, &finnhub.CompanyProfile2Opts{Symbol: optional.NewString(string(req.Symbol))})
			if err != nil {
				return handleErr(fmt.Sprintf("error while getting company profile %q", req.Symbol), httpResp, err)
			}
			result = CompanyProfileResponse{Request: req, Response: profiles}
			return nil
		}
	}, bo, bon)
	return
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

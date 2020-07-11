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
	"net/http"
	"time"
)

func ExtractStocks(ctx context.Context, des chan<- Stock, apiKey, exchange string) error {
	c := finnhub.NewAPIClient(finnhub.NewConfiguration()).DefaultApi
	ctx = context.WithValue(ctx, finnhub.ContextAPIKey, finnhub.APIKey{Key: apiKey})

	ss, _, err := c.StockSymbols(ctx, exchange)
	if err != nil {
		return fmt.Errorf("error while getting stocks: %w", err)
	}

	for _, stock := range ss {
		des <- mapStock(stock)
	}

	return nil
}

type Stock struct {
	Description   string `yaml:"description,omitempty" json:"description,omitempty"`
	DisplaySymbol string `yaml:"display_symbol,omitempty" json:"display_symbol,omitempty"`
	Symbol        string `yaml:"symbol,omitempty" json:"symbol,omitempty"`
}

func mapStock(s finnhub.Stock) Stock {
	return Stock{
		s.Description,
		s.DisplaySymbol,
		s.Symbol,
	}
}

func ExtractStockCandles(ctx context.Context, des chan<- StockCandles, apiKey string, stock Stock, resolution string, from, to time.Time) error {
	c := finnhub.NewAPIClient(finnhub.NewConfiguration()).DefaultApi
	ctx = context.WithValue(ctx, finnhub.ContextAPIKey, finnhub.APIKey{Key: apiKey})
	var retry bool
startRequest:
	select {
	case <-ctx.Done():
		return fmt.Errorf("error while getting candle for stock %q: %w", stock.Symbol, ctx.Err())
	default:
		stockCandles, resp, err := c.StockCandles(ctx, stock.Symbol, resolution, from.Unix(), to.Unix(), nil)
		if err != nil {
			if !retry && resp.StatusCode == http.StatusTooManyRequests {
				retry = true
				time.Sleep(time.Second)
				goto startRequest
			}
			retry = false
			return fmt.Errorf("error while getting candle for stock %q: %w", stock.Symbol, err)
		}
		des <- mapStockCandles(stockCandles)
	}

	return nil
}

type StockCandles struct {
	Open      []float32   `yaml:"open,omitempty" json:"open,omitempty"`
	High      []float32   `yaml:"high,omitempty" json:"high,omitempty"`
	Low       []float32   `yaml:"low,omitempty" json:"low,omitempty"`
	Close     []float32   `yaml:"close,omitempty" json:"close,omitempty"`
	Volume    []float32   `yaml:"volume,omitempty" json:"volume,omitempty"`
	Timestamp []time.Time `yaml:"timestamp,omitempty" json:"timestamp,omitempty"`
	NoData    bool        `yaml:"no_data,omitempty" json:"no_data,omitempty"`
}

func mapStockCandles(s finnhub.StockCandles) StockCandles {
	return StockCandles{
		s.O,
		s.H,
		s.L,
		s.C,
		s.V,
		mapTimestamps(s.T),
		s.S == "no_data",
	}
}

func mapTimestamps(ts []int64) []time.Time {
	result := make([]time.Time, len(ts))
	for i, t := range ts {
		result[i] = mapTimestamp(t)
	}
	return result
}

func mapTimestamp(t int64) time.Time {
	return time.Unix(t, 0)
}

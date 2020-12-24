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

package transform

import (
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/stocker/internal/external"
	"time"

	"github.com/ajjensen13/stocker/internal/model"
)

func Stocks(in []finnhub.Stock) []model.Stock {
	out := make([]model.Stock, len(in))
	for i, es := range in {
		out[i] = Stock(es)
	}
	return out
}

func Stock(s finnhub.Stock) (out model.Stock) {
	_ = out.Symbol.Set(s.Symbol)
	_ = out.DisplaySymbol.Set(s.DisplaySymbol)
	_ = out.Description.Set(s.Description)
	return
}

func StockCandles(in []external.StockCandlesWithMetadata, tz *time.Location) ([][]model.Candle, error) {
	ret := make([][]model.Candle, len(in))
	for i, cin := range in {
		c, err := Candles(cin.Symbol, cin.StockCandles, tz)
		if err != nil {
			return nil, fmt.Errorf("failed to transform %s stock candles: %w", cin.Symbol, err)
		}
		ret[i] = c
	}
	return ret, nil
}

func Candles(symbol string, in finnhub.StockCandles, tz *time.Location) ([]model.Candle, error) {
	l := len(in.T)
	switch {
	case l == 0:
		return nil, nil
	case len(in.O) != l:
		return nil, fmt.Errorf("len(open) = %d, len(timestamp) = %d for stock %q", len(in.O), l, symbol)
	case len(in.H) != l:
		return nil, fmt.Errorf("len(high) = %d, len(timestamp) = %d for stock %q", len(in.H), l, symbol)
	case len(in.L) != l:
		return nil, fmt.Errorf("len(low) = %d, len(timestamp) = %d for stock %q", len(in.L), l, symbol)
	case len(in.C) != l:
		return nil, fmt.Errorf("len(close) = %d, len(timestamp) = %d for stock %q", len(in.C), l, symbol)
	case len(in.V) != l:
		return nil, fmt.Errorf("len(volume) = %d, len(timestamp) = %d for stock %q", len(in.V), l, symbol)
	}

	result := make([]model.Candle, l)
	for ndx, ts := range in.T {
		_ = result[ndx].Symbol.Set(symbol)
		_ = result[ndx].Timestamp.Set(time.Unix(ts, 0).In(tz))
		_ = result[ndx].Open.Set(in.O[ndx])
		_ = result[ndx].High.Set(in.H[ndx])
		_ = result[ndx].Low.Set(in.L[ndx])
		_ = result[ndx].Close.Set(in.C[ndx])
		_ = result[ndx].Volume.Set(in.V[ndx])
	}

	return result, nil
}

func CompanyProfiles(in []finnhub.CompanyProfile2) ([]model.CompanyProfile, error) {
	out := make([]model.CompanyProfile, len(in))
	for i, icp := range in {
		ocp, err := CompanyProfile(icp)
		if err != nil {
			return nil, fmt.Errorf("failed to transform %s company profile: %w", icp.Ticker, err)
		}
		out[i] = ocp
	}
	return out, nil
}

func CompanyProfile(in finnhub.CompanyProfile2) (out model.CompanyProfile, err error) {
	_ = out.Country.Set(in.Country)
	_ = out.Currency.Set(in.Currency)
	_ = out.Exchange.Set(in.Exchange)
	_ = out.Name.Set(in.Name)
	_ = out.Symbol.Set(in.Ticker)
	_ = out.Ipo.Set(in.Ipo)
	_ = out.MarketCapitalization.Set(in.MarketCapitalization)
	_ = out.SharesOutstanding.Set(in.ShareOutstanding)
	_ = out.Logo.Set(in.Logo)
	_ = out.Phone.Set(in.Phone)
	_ = out.WebUrl.Set(in.Weburl)
	_ = out.Industry.Set(in.FinnhubIndustry)
	return
}

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
	"time"

	"github.com/ajjensen13/stocker/internal/model"
)

func Stock(s finnhub.Stock) model.Stock {
	return model.Stock{
		Symbol:        s.Symbol,
		DisplaySymbol: s.DisplaySymbol,
		Description:   s.Description,
	}
}

func Candles(s finnhub.Stock, in finnhub.StockCandles, tz *time.Location) ([]model.Candle, error) {
	l := len(in.T)
	switch {
	case l == 0:
		return nil, nil
	case len(in.O) != l:
		return nil, fmt.Errorf("len(open) = %d, len(timestamp) = %d for stock %q", len(in.O), l, s.Symbol)
	case len(in.H) != l:
		return nil, fmt.Errorf("len(high) = %d, len(timestamp) = %d for stock %q", len(in.H), l, s.Symbol)
	case len(in.L) != l:
		return nil, fmt.Errorf("len(low) = %d, len(timestamp) = %d for stock %q", len(in.L), l, s.Symbol)
	case len(in.C) != l:
		return nil, fmt.Errorf("len(close) = %d, len(timestamp) = %d for stock %q", len(in.C), l, s.Symbol)
	case len(in.V) != l:
		return nil, fmt.Errorf("len(volume) = %d, len(timestamp) = %d for stock %q", len(in.V), l, s.Symbol)
	}

	result := make([]model.Candle, l)
	for ndx, ts := range in.T {
		result[ndx] = model.Candle{
			Symbol:    s.Symbol,
			Timestamp: time.Unix(ts, 0).In(tz),
			Open:      &in.O[ndx],
			High:      &in.H[ndx],
			Low:       &in.L[ndx],
			Close:     &in.C[ndx],
			Volume:    &in.V[ndx],
		}
	}

	return result, nil
}

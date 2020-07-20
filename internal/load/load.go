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

package load

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"

	"github.com/ajjensen13/stocker/internal/model"
)

func Stocks(ctx context.Context, tx pgx.Tx, ss []model.Stock) error {
	for _, stock := range ss {
		_, err := tx.Exec(ctx, `INSERT INTO "public"."Stocks" ("Symbol", "DisplaySymbol", "Description") VALUES ($1, $2, $3) ON CONFLICT ("Symbol") DO UPDATE SET "DisplaySymbol" = $2, "Description" = $3`, stock.Symbol, stock.DisplaySymbol, stock.Description)
		if err != nil {
			return fmt.Errorf("failed to load stock symbol %q: %w", stock.Symbol, err)
		}
	}
	return nil
}

func Candles(ctx context.Context, tx pgx.Tx, ss []model.Candle) error {
	for _, s := range ss {
		_, err := tx.Exec(ctx, `INSERT INTO "public"."Candles" ("Symbol", "Timestamp", "Open", "High", "Low", "Close", "Volume") VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT ("Symbol", "Timestamp") DO UPDATE SET "Open" = $3, "High" = $4, "Low" = $5, "Close" = $6, "Volume" = $7`, s.Symbol, s.Timestamp, s.Open, s.High, s.Low, s.Close, s.Volume)
		if err != nil {
			return fmt.Errorf("failed to load stock symbol %q: %w", s.Symbol, err)
		}
	}
	return nil
}

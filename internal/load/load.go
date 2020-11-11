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
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"

	"github.com/ajjensen13/stocker/internal/model"
	"github.com/ajjensen13/stocker/internal/util"
)

func Stocks(ctx context.Context, tx pgx.Tx, ss []model.Stock, bo backoff.BackOff, bon backoff.Notify) error {
	return backoff.RetryNotify(util.WrapWithSavePoint(ctx, tx, func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, util.MedReqTimeout)
		defer cancel()

		for _, stock := range ss {
			_, err := tx.Exec(ctx, `INSERT INTO src."Stocks" ("Symbol", "DisplaySymbol", "Description", "Created", "Modified") VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT ("Symbol") DO UPDATE SET "DisplaySymbol" = $2, "Description" = $3, "Modified" = CURRENT_TIMESTAMP WHERE "Stocks"."DisplaySymbol" <> $2 OR "Stocks"."Description" <> $3`, stock.Symbol, stock.DisplaySymbol, stock.Description)
			if err != nil {
				return fmt.Errorf("failed to load stock symbol %q: %w", stock.Symbol, err)
			}
		}
		return nil
	}, "load_stocks"), bo, bon)
}

func Candles(ctx context.Context, tx pgx.Tx, ss []model.Candle, bo backoff.BackOff, bon backoff.Notify) error {
	return backoff.RetryNotify(util.WrapWithSavePoint(ctx, tx, func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, util.MedReqTimeout)
		defer cancel()

		for _, s := range ss {
			_, err := tx.Exec(ctx, `INSERT INTO src."Candles" ("Symbol", "Timestamp", "Open", "High", "Low", "Close", "Volume", "Created", "Modified") VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT ("Symbol", "Timestamp") DO UPDATE SET "Open" = $3, "High" = $4, "Low" = $5, "Close" = $6, "Volume" = $7, "Modified" = CURRENT_TIMESTAMP WHERE "Candles"."Open" <> $3 OR "Candles"."High" <> $4 OR "Candles"."Low" <> $5 OR "Candles"."Close" <> $6 OR "Candles"."Volume" <> $7`, s.Symbol, s.Timestamp, s.Open, s.High, s.Low, s.Close, s.Volume)
			if err != nil {
				return fmt.Errorf("failed to load stock symbol %q: %w", s.Symbol, err)
			}
		}
		return nil
	}, "load_candles"), bo, bon)
}

func CompanyProfile(ctx context.Context, tx pgx.Tx, cp model.CompanyProfile, bo backoff.BackOff, bon backoff.Notify) error {
	return backoff.RetryNotify(util.WrapWithSavePoint(ctx, tx, func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, util.ShortReqTimeout)
		defer cancel()

		_, err = tx.Exec(ctx, `INSERT INTO src."CompanyProfiles" ("Symbol", "Exchange", "Country", "Currency", "Name", "Ipo", "MarketCapitalization", "SharesOutstanding", "Logo", "Phone", "WebUrl", "Industry", "Created", "Modified") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT ("Symbol") DO UPDATE SET "Exchange" = $2, "Country" = $3, "Currency" = $4, "Name" = $5, "Ipo" = $6, "MarketCapitalization" = $7, "SharesOutstanding" = $8, "Logo" = $9, "Phone" = $10, "WebUrl" = $11, "Industry" = $12, "Modified" = CURRENT_TIMESTAMP WHERE "CompanyProfiles"."Exchange" <> $2 OR "CompanyProfiles"."Country" <> $3 OR "CompanyProfiles"."Currency" <> $4 OR "CompanyProfiles"."Name" <> $5 OR "CompanyProfiles"."Ipo" <> $6 OR "CompanyProfiles"."MarketCapitalization" <> $7 OR "CompanyProfiles"."SharesOutstanding" <> $8 OR "CompanyProfiles"."Logo" <> $9 OR "CompanyProfiles"."Phone" <> $10 OR "CompanyProfiles"."WebUrl" <> $11 OR "CompanyProfiles"."Industry" <> $12`, cp.Symbol, cp.Exchange, cp.Country, cp.Currency, cp.Name, cp.Ipo, cp.MarketCapitalization, cp.ShareOutstanding, cp.Logo, cp.Phone, cp.WebUrl, cp.Industry)
		if err != nil {
			return fmt.Errorf("failed to load company profile %q: %w", cp.Symbol, err)
		}
		return nil
	}, "load_company_profiles"), bo, bon)
}

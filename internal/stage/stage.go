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

package stage

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"time"
)

func latestModification(ctx context.Context, tx pgx.Tx, table string) (time.Time, error) {
	rows, err := tx.Query(ctx, `SELECT MAX("Modified") FROM `+table)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to query latest %s modification: %w", table, err)
	}

	if !rows.Next() {
		return time.Time{}, nil
	}

	var timestamp time.Time
	err = rows.Scan(&timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse latest %s modification: %w", table, err)
	}

	if rows.Next() {
		return time.Time{}, fmt.Errorf("query for latest %s modification returned multiple rows", table)
	}

	return timestamp, nil
}

func Stocks(ctx context.Context, tx pgx.Tx) (StagingInfo, error) {
	l, err := latestModification(ctx, tx, `stage."Stocks"`)
	if err != nil {
		return StagingInfo{}, err
	}

	r, err := tx.Exec(ctx, `INSERT INTO stage."Stocks" ("Symbol", "DisplaySymbol", "Description", "Created", "Modified") SELECT "Symbol", "DisplaySymbol", "Description", "Created", "Modified" FROM src."Stocks" WHERE "Modified" > $1 ON CONFLICT ("Symbol") DO UPDATE SET "DisplaySymbol" = excluded."DisplaySymbol", "Description" = excluded."Description", "Created" = excluded."Created", "Modified" = excluded."Modified"`, l)
	if err != nil {
		return StagingInfo{}, fmt.Errorf("error while staging stocks: %w", err)
	}

	return StagingInfo{PreviousLatestModification: l, RowsAffected: r.RowsAffected()}, nil
}

func Candles(ctx context.Context, tx pgx.Tx) (StagingInfo, error) {
	l, err := latestModification(ctx, tx, `stage."Candles"`)
	if err != nil {
		return StagingInfo{}, err
	}

	r, err := tx.Exec(ctx, `INSERT INTO stage."Candles" ("Symbol", "Timestamp", "Open", "High", "Low", "Close", "Volume", "Created", "Modified") SELECT "Symbol", "Timestamp", "Open", "High", "Low", "Close", "Volume", "Created", "Modified" FROM src."Candles" WHERE "Modified" > $1 ON CONFLICT ("Symbol", "Timestamp") DO UPDATE SET "Open" = excluded."Open", "High" = excluded."High", "Low" = excluded."Low", "Close" = excluded."Close", "Volume" = excluded."Volume", "Created" = excluded."Created", "Modified" = excluded."Modified"`, l)
	if err != nil {
		return StagingInfo{}, fmt.Errorf("error while staging candles: %w", err)
	}

	return StagingInfo{PreviousLatestModification: l, RowsAffected: r.RowsAffected()}, nil
}

func CompanyProfile(ctx context.Context, tx pgx.Tx) (StagingInfo, error) {
	l, err := latestModification(ctx, tx, `stage."CompanyProfiles"`)
	if err != nil {
		return StagingInfo{}, err
	}

	r, err := tx.Exec(ctx, `INSERT INTO stage."CompanyProfiles" ("Exchange", "Symbol", "Country", "Currency", "Name", "Ipo", "MarketCapitalization", "SharesOutstanding", "Logo", "Phone", "WebUrl", "Industry", "Created", "Modified") SELECT "Exchange", "Symbol", "Country", "Currency", "Name", "Ipo", "MarketCapitalization", "SharesOutstanding", "Logo", "Phone", "WebUrl", "Industry", "Created", "Modified" FROM src."CompanyProfiles" WHERE "Modified" > $1 ON CONFLICT ("Exchange", "Symbol") DO UPDATE SET "Country" = excluded."Country", "Currency" = excluded."Currency", "Name" = excluded."Name", "Ipo" = excluded."Ipo", "MarketCapitalization" = excluded."MarketCapitalization", "SharesOutstanding" = excluded."SharesOutstanding", "Logo" = excluded."Logo", "Phone" = excluded."Phone", "WebUrl" = excluded."WebUrl", "Industry" = excluded."Industry", "Created" = excluded."Created", "Modified" = excluded."Modified"`, l)
	if err != nil {
		return StagingInfo{}, fmt.Errorf("error while staging company profiles: %w", err)
	}

	return StagingInfo{PreviousLatestModification: l, RowsAffected: r.RowsAffected()}, nil
}

func Candles52Wk(ctx context.Context, tx pgx.Tx, latestModification time.Time) (StagingInfo, error) {
	r, err := tx.Exec(ctx, `WITH affected AS (SELECT DISTINCT affected."Symbol", affected."Timestamp" FROM src."Candles" modified JOIN src."Candles" affected ON modified."Symbol" = affected."Symbol" WHERE modified."Modified" > $1 AND modified."Timestamp" BETWEEN affected."Timestamp" - INTERVAL '52 week' AND affected."Timestamp") INSERT INTO stage."Candles52Wk" ("Symbol", "Timestamp", "HighMax", "HighMin", "HighAvg", "HighCount", "LowMax", "LowMin", "LowAvg", "LowCount", "VolumeMax", "VolumeMin", "VolumeAvg", "VolumeCount") SELECT "Candles52Wk"."Symbol", "Candles52Wk"."Timestamp", "Candles52Wk"."HighMax", "Candles52Wk"."HighMin", "Candles52Wk"."HighAvg", "Candles52Wk"."HighCount", "Candles52Wk"."LowMax", "Candles52Wk"."LowMin", "Candles52Wk"."LowAvg", "Candles52Wk"."LowCount", "Candles52Wk"."VolumeMax", "Candles52Wk"."VolumeMin", "Candles52Wk"."VolumeAvg", "Candles52Wk"."VolumeCount" FROM src."Candles52Wk" JOIN affected ON affected."Symbol" = "Candles52Wk"."Symbol" AND affected."Timestamp" = "Candles52Wk"."Timestamp" ON CONFLICT ("Symbol", "Timestamp") DO UPDATE SET "Symbol" = excluded."Symbol", "Timestamp" = excluded."Timestamp", "HighMax" = excluded."HighMax", "HighMin" = excluded."HighMin", "HighAvg" = excluded."HighAvg", "HighCount" = excluded."HighCount", "LowMax" = excluded."LowMax", "LowMin" = excluded."LowMin", "LowAvg" = excluded."LowAvg", "LowCount" = excluded."LowCount", "VolumeMax" = excluded."VolumeMax", "VolumeMin" = excluded."VolumeMin", "VolumeAvg" = excluded."VolumeAvg", "VolumeCount" = excluded."VolumeCount"`, latestModification)
	if err != nil {
		return StagingInfo{}, fmt.Errorf("error while staging 52 week candles: %w", err)
	}

	return StagingInfo{PreviousLatestModification: latestModification, RowsAffected: r.RowsAffected()}, nil
}

type StagingInfo struct {
	PreviousLatestModification time.Time
	RowsAffected               int64
}

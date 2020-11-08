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
	"github.com/ajjensen13/gke"
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

	var timestamp *time.Time
	err = rows.Scan(&timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse latest %s modification: %w", table, err)
	}

	if rows.Next() {
		return time.Time{}, fmt.Errorf("query for latest %s modification returned multiple rows", table)
	}

	if timestamp == nil {
		return time.Time{}, nil
	}

	return *timestamp, nil
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

func Candles52Wk(ctx context.Context, lg gke.Logger, tx pgx.Tx, latestModification time.Time) (StagingInfo, error) {
	affected, err := calcAffected(ctx, tx, latestModification)
	if err != nil {
		return StagingInfo{}, err
	}
	lg.Defaultf("successfully calculated affected 52wk candles")

	err = updateAffected(ctx, lg, tx, affected)
	if err != nil {
		return StagingInfo{}, err
	}

	return StagingInfo{PreviousLatestModification: latestModification, RowsAffected: int64(len(affected))}, nil
}

func updateAffected(ctx context.Context, lg gke.Logger, tx pgx.Tx, affected map[string][]time.Time) error {
	var updates uint

	for symbol, timestamps := range affected {
		for _, timestamp := range timestamps {
			_, err := tx.Exec(ctx, `
			INSERT INTO stage."Candles52Wk" ("Symbol", "Timestamp", "HighMax", "HighMin", "HighAvg", "HighCount", "LowMax", "LowMin", "LowAvg", "LowCount", "VolumeMax", "VolumeMin", "VolumeAvg", "VolumeCount") 
			SELECT 
				"Candles52Wk"."Symbol", 
				"Candles52Wk"."Timestamp", 
				"Candles52Wk"."HighMax", 
				"Candles52Wk"."HighMin", 
				"Candles52Wk"."HighAvg", 
				"Candles52Wk"."HighCount", 
				"Candles52Wk"."LowMax", 
				"Candles52Wk"."LowMin", 
				"Candles52Wk"."LowAvg", 
				"Candles52Wk"."LowCount", 
				"Candles52Wk"."VolumeMax", 
				"Candles52Wk"."VolumeMin", 
				"Candles52Wk"."VolumeAvg", 
				"Candles52Wk"."VolumeCount" 
			FROM src."Candles52Wk" 
			WHERE 
				"Candles52Wk"."Symbol" = $1
				AND "Candles52Wk"."Timestamp" = $2
			ON CONFLICT ("Symbol", "Timestamp") 
			DO UPDATE SET 
				"Symbol" = excluded."Symbol", 
				"Timestamp" = excluded."Timestamp", 
				"HighMax" = excluded."HighMax", 
				"HighMin" = excluded."HighMin", 
				"HighAvg" = excluded."HighAvg", 
				"HighCount" = excluded."HighCount", 
				"LowMax" = excluded."LowMax", 
				"LowMin" = excluded."LowMin", 
				"LowAvg" = excluded."LowAvg", 
				"LowCount" = excluded."LowCount", 
				"VolumeMax" = excluded."VolumeMax", 
				"VolumeMin" = excluded."VolumeMin", 
				"VolumeAvg" = excluded."VolumeAvg", 
				"VolumeCount" = excluded."VolumeCount"
			`, symbol, timestamp)

			if err != nil {
				return fmt.Errorf("failed to update 52 week candle %v %v: %w", symbol, timestamp, err)
			}

			updates++
		}
		lg.Default(struct {
			Message    string
			Symbol     string
			Timestamps []time.Time
		}{
			Message:    fmt.Sprintf("successfully updated %d affected 52wk candles for symbol %s", len(timestamps), symbol),
			Symbol:     symbol,
			Timestamps: timestamps,
		})
	}

	lg.Defaultf("successfully updated %d affected 52wk candles for %d symbols", updates, len(affected))
	return nil
}

func calcAffected(ctx context.Context, tx pgx.Tx, latestModification time.Time) (map[string][]time.Time, error) {
	rs, err := tx.Query(ctx, `
		SELECT
			affected."Symbol",
			affected."Timestamp"
		FROM src."Candles" modified
		JOIN src."Candles" affected
			ON modified."Symbol" = affected."Symbol"
			AND affected."Timestamp" BETWEEN modified."Timestamp" - INTERVAL '52 week' AND modified."Timestamp" 
		WHERE
			modified."Timestamp" > $1
		GROUP BY
			affected."Symbol",
			affected."Timestamp"
		`, latestModification)
	if err != nil {
		return nil, fmt.Errorf("error while determining affected 52 week candles: %w", err)
	}
	defer rs.Close()

	var ret = map[string][]time.Time{}
	for rs.Next() {
		var symbol string
		var timestamp time.Time
		err := rs.Scan(&symbol, &timestamp)
		if err != nil {
			return nil, fmt.Errorf("error while reading affected 52 week candles: %w", err)
		}

		if _, ok := ret[symbol]; !ok {
			ret[symbol] = []time.Time{timestamp}
			continue
		}

		ret[symbol] = append(ret[symbol], timestamp)
	}

	return ret, nil
}

type StagingInfo struct {
	PreviousLatestModification time.Time
	RowsAffected               int64
}

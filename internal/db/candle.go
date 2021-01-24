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

package db

import (
	"cloud.google.com/go/logging"
	"context"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/stocker/internal/api"
	"github.com/ajjensen13/stocker/internal/util"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
)

type Candle struct {
	Symbol    pgtype.Text
	Timestamp pgtype.Timestamptz
	Open      pgtype.Float4
	High      pgtype.Float4
	Low       pgtype.Float4
	Close     pgtype.Float4
	Volume    pgtype.Float4
}

type CompanyProfile struct {
	Symbol               pgtype.Text
	Country              pgtype.Text
	Currency             pgtype.Text
	Exchange             pgtype.Text
	Name                 pgtype.Text
	Ticker               pgtype.Text
	Ipo                  pgtype.Date
	MarketCapitalization pgtype.Float4
	SharesOutstanding    pgtype.Float4
	Logo                 pgtype.Text
	Phone                pgtype.Text
	WebUrl               pgtype.Text
	Industry             pgtype.Text
}

type Stock struct {
	Symbol        pgtype.Text
	DisplaySymbol pgtype.Text
	Description   pgtype.Text
}

func TransformStocks(in []finnhub.Stock) (out []Stock) {
	out = make([]Stock, len(in))
	for i, es := range in {
		out[i] = TransformStock(es)
	}
	return out
}

func TransformStock(s finnhub.Stock) (out Stock) {
	_ = out.Symbol.Set(s.Symbol)
	_ = out.DisplaySymbol.Set(s.DisplaySymbol)
	_ = out.Description.Set(s.Description)
	return
}

func TransformStockCandles(in []api.CandlesResponse, tz *time.Location) (out [][]Candle, err error) {
	ret := make([][]Candle, len(in))
	for i, candle := range in {
		c, err := TransformCandles(candle.Request.Symbol, candle.Response, tz)
		if err != nil {
			return nil, fmt.Errorf("failed to transform %s stock candles: %w", candle.Request.Symbol, err)
		}
		ret[i] = c
	}
	return ret, nil
}

func TransformCandles(symbol api.Symbol, in finnhub.StockCandles, tz *time.Location) (out []Candle, err error) {
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

	out = make([]Candle, l)
	for ndx, ts := range in.T {
		_ = out[ndx].Symbol.Set(string(symbol))
		_ = out[ndx].Timestamp.Set(time.Unix(ts, 0).In(tz))
		_ = out[ndx].Open.Set(in.O[ndx])
		_ = out[ndx].High.Set(in.H[ndx])
		_ = out[ndx].Low.Set(in.L[ndx])
		_ = out[ndx].Close.Set(in.C[ndx])
		_ = out[ndx].Volume.Set(in.V[ndx])
	}

	return out, nil
}

func TransformCompanyProfile(symbol api.Symbol, in finnhub.CompanyProfile2) (out CompanyProfile) {
	_ = out.Symbol.Set(string(symbol))
	_ = out.Country.Set(in.Country)
	_ = out.Currency.Set(in.Currency)
	_ = out.Exchange.Set(in.Exchange)
	_ = out.Name.Set(in.Name)
	_ = out.Ticker.Set(in.Ticker)
	_ = out.Ipo.Set(in.Ipo)
	_ = out.MarketCapitalization.Set(in.MarketCapitalization)
	_ = out.SharesOutstanding.Set(in.ShareOutstanding)
	_ = out.Logo.Set(in.Logo)
	_ = out.Phone.Set(in.Phone)
	_ = out.WebUrl.Set(in.Weburl)
	_ = out.Industry.Set(in.FinnhubIndustry)
	return
}

func SaveStocks(ctx context.Context, pool *pgxpool.Pool, jobRunId uint64, bo backoff.BackOff, bon backoff.Notify, stocks api.StocksResponse) error {
	ctx = util.WithLoggerValue(ctx, "action", "load")
	return backoff.RetryNotify(func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		return util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			success := 0

			for _, stock := range stocks.Response {
				_, err := tx.Exec(ctx, `INSERT INTO src.stocks (job_run_id, symbol, data) VALUES ($1, $2, $3)`, jobRunId, stock.Symbol, stock)
				if err != nil {
					return fmt.Errorf("failed to load stock symbol %q: %w", stock.Symbol, err)
				}
				success++
			}

			util.Logf(ctx, logging.Debug, "successfully inserted %d of %d stocks into src.stocks", success, len(stocks.Response))
			return nil
		})
	}, bo, bon)
}

func SaveCandles(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, bo backoff.BackOff, bon backoff.Notify, candles api.CandlesResponse) error {
	ctx = util.WithLoggerValue(ctx, "action", "load")
	return backoff.RetryNotify(func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		return util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			_, err = tx.Exec(ctx, `INSERT INTO src.candles (job_run_id, symbol, "from", "to", data) VALUES ($1, $2, $3, $4, $5)`, jobRunId, candles.Request.Symbol, candles.Request.From, candles.Request.To, candles.Response)
			if err != nil {
				return fmt.Errorf("failed to load stock symbol %q: %w", candles.Request.Symbol, err)
			}
			return nil
		})
	}, bo, bon)
}

func SaveCompanyProfile(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, bo backoff.BackOff, bon backoff.Notify, profiles api.CompanyProfileResponse) error {
	ctx = util.WithLoggerValue(ctx, "action", "load")
	return backoff.RetryNotify(func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		return util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			_, err = tx.Exec(ctx, `INSERT INTO src.company_profiles (job_run_id, symbol, data) VALUES ($1, $2, $3)`, jobRunId, profiles.Request.Symbol, profiles.Response)
			if err != nil {
				return fmt.Errorf("failed to load company profile %q: %w", profiles.Request.Symbol, err)
			}
			return nil
		})
	}, bo, bon)
}

func StageStocks(ctx context.Context, pool *pgxpool.Pool, jobRunId uint64, bo backoff.BackOff, bon backoff.Notify) (ret StagingInfo, err error) {
	ctx = util.WithLoggerValue(ctx, "action", "stage")

	err = backoff.RetryNotify(func() error {
		var rowsStaged, rowsModified int64

		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		err := util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			srcStocks, err := lookupStocksToStage(ctx, jobRunId, tx)
			if err != nil {
				return err
			}

			stocks := TransformStocks(srcStocks)

			summary := make(map[string]bool, len(stocks))
			for _, stock := range stocks {
				ctx := util.WithLoggerValue(ctx, "symbol", stock.Symbol)

				sql := `
					INSERT INTO stage.stocks
						(job_run_id, symbol, display_symbol, description, created, modified) 
					VALUES 
						($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
					ON CONFLICT 
						(symbol) 
					DO UPDATE 
						SET 
							job_run_id = excluded.job_run_id,
							display_symbol = excluded.display_symbol,
							description = excluded.description,
							modified = excluded.modified
						WHERE
							stocks.display_symbol IS DISTINCT FROM excluded.display_symbol OR
							stocks.description IS DISTINCT FROM excluded.description`

				r, err := tx.Exec(ctx, sql, jobRunId, stock.Symbol, stock.DisplaySymbol, stock.Description)
				if err != nil {
					return fmt.Errorf("error while staging stocks: %w", err)
				}

				rowsModified += r.RowsAffected()
				rowsStaged++

				summary[stock.Symbol.String] = r.RowsAffected() > 0
			}

			util.Logf(util.WithLoggerValue(ctx, "stock_stage_info", summary), logging.Debug, "successfully staged stocks")

			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to stage stocks: %w", err)
		}

		ret = StagingInfo{RowsModified: rowsModified, RowsStaged: rowsStaged}
		return nil
	}, bo, bon)

	return
}

func lookupStocksToStage(ctx context.Context, jobRunId uint64, tx pgx.Tx) (ret []finnhub.Stock, err error) {
	rows, err := tx.Query(ctx, `SELECT data FROM src.stocks WHERE job_run_id = $1`, jobRunId)
	if err != nil {
		return nil, fmt.Errorf("failed to get source stocks: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var src finnhub.Stock
		err := rows.Scan(&src)
		if err != nil {
			return nil, fmt.Errorf("failed to scan source stocks: %w", err)
		}
		ret = append(ret, src)
	}

	return ret, nil
}

func StageCandles(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, bo backoff.BackOff, bon backoff.Notify, tz *time.Location) (ret StagingInfo, err error) {
	ctx = util.WithLoggerValue(ctx, "action", "stage")
	err = backoff.RetryNotify(func() error {
		var rowsStaged, rowsModified int64
		var srcCandles []api.CandlesResponse

		err := util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) (err error) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			srcCandles, err = lookupCandlesToStage(ctx, jobRunId, tx)
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to query candles to stage: %w", err)
		}

		err = util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()

			candles, err := TransformStockCandles(srcCandles, tz)
			if err != nil {
				return err
			}

			for _, stockCandles := range candles {
				summary := map[string]int{}

				for _, stockCandle := range stockCandles {
					ctx := util.WithLoggerValue(ctx, "symbol", stockCandle.Symbol)

					sql := `
						INSERT INTO stage.candles
							(job_run_id, symbol, timestamp, open, high, low, close, volume, modified, created) 
						VALUES 
							($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
						ON CONFLICT 
							(symbol, timestamp) 
						DO UPDATE 
							SET 
								job_run_id = excluded.job_run_id,
								open = excluded.open,
								high = excluded.high,
								low = excluded.low,
								close = excluded.close,
								volume = excluded.volume,
								modified = excluded.modified
							WHERE
								candles.open IS DISTINCT FROM excluded.open OR
								candles.high IS DISTINCT FROM excluded.high OR
								candles.low IS DISTINCT FROM excluded.low OR
								candles.close IS DISTINCT FROM excluded.close OR
								candles.volume IS DISTINCT FROM excluded.volume`

					r, err := tx.Exec(ctx, sql, jobRunId, stockCandle.Symbol, stockCandle.Timestamp, stockCandle.Open, stockCandle.High, stockCandle.Low, stockCandle.Close, stockCandle.Volume)
					if err != nil {
						return fmt.Errorf("error while staging candles: %w", err)
					}

					rowsModified += r.RowsAffected()
					rowsStaged++

					summary[stockCandle.Symbol.String] = 1 + summary[stockCandle.Symbol.String]
				}

				util.Logf(util.WithLoggerValue(ctx, "candle_stage_info", summary), logging.Debug, "successfully staged %d candles", len(candles))
			}

			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to stage candles: %w", err)
		}

		ret = StagingInfo{RowsModified: rowsModified, RowsStaged: rowsStaged}
		return nil
	}, bo, bon)

	return
}

func lookupCandlesToStage(ctx context.Context, jobRunId uint64, tx pgx.Tx) (ret []api.CandlesResponse, err error) {
	rows, err := tx.Query(ctx, `SELECT symbol, data FROM src.candles WHERE job_run_id = $1`, jobRunId)
	if err != nil {
		return nil, fmt.Errorf("failed to get source candles: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var d api.CandlesResponse
		err := rows.Scan(&d.Request.Symbol, &d.Response)
		if err != nil {
			return nil, fmt.Errorf("failed to scan source candles: %w", err)
		}
		ret = append(ret, d)
	}

	return ret, nil
}

func StageCompanyProfiles(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, bo backoff.BackOff, bon backoff.Notify) (ret StagingInfo, err error) {
	ctx = util.WithLoggerValue(ctx, "action", "stage")

	err = backoff.RetryNotify(func() error {
		var rowsStaged, rowsModified int64
		var responses []api.CompanyProfileResponse

		err := util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) (err error) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			responses, err = lookupCompanyProfilesToStage(ctx, jobRunId, tx)
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to query company profiles to stage: %w", err)
		}

		err = util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()

			for _, response := range responses {
				ctx := util.WithLoggerValue(ctx, "symbol", response.Request.Symbol)

				profile := TransformCompanyProfile(response.Request.Symbol, response.Response)

				sql := `
					INSERT INTO stage.company_profiles
						(job_run_id, symbol, exchange, country, currency, name, ticker, ipo, market_capitalization, shares_outstanding, logo, phone, web_url, industry, created, modified)
					VALUES
						($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) 
					ON CONFLICT 
						(symbol) 
					DO UPDATE 
						SET 
							job_run_id = excluded.job_run_id,
							exchange = excluded.exchange, 
							country = excluded.country, 
							currency = excluded.currency, 
							name = excluded.name, 
							ticker = excluded.ticker, 
							ipo = excluded.ipo, 
							market_capitalization = excluded.market_capitalization, 
							shares_outstanding = excluded.shares_outstanding, 
							logo = excluded.logo, 
							phone = excluded.phone, 
							web_url = excluded.web_url, 
							industry = excluded.industry, 
							modified = excluded.modified
						WHERE
							company_profiles.exchange IS DISTINCT FROM excluded.exchange OR
							company_profiles.country IS DISTINCT FROM excluded.country OR
							company_profiles.currency IS DISTINCT FROM excluded.currency OR
							company_profiles.name IS DISTINCT FROM excluded.name OR
							company_profiles.ticker IS DISTINCT FROM excluded.ticker OR
							company_profiles.ipo IS DISTINCT FROM excluded.ipo OR
							company_profiles.market_capitalization IS DISTINCT FROM excluded.market_capitalization OR
							company_profiles.shares_outstanding IS DISTINCT FROM excluded.shares_outstanding OR
							company_profiles.logo IS DISTINCT FROM excluded.logo OR
							company_profiles.phone IS DISTINCT FROM excluded.phone OR
							company_profiles.web_url IS DISTINCT FROM excluded.web_url OR
							company_profiles.industry IS DISTINCT FROM excluded.industry
`

				r, err := tx.Exec(ctx, sql, jobRunId, response.Request.Symbol, profile.Exchange, profile.Country, profile.Currency, profile.Name, profile.Ticker, profile.Ipo, profile.MarketCapitalization, profile.SharesOutstanding, profile.Logo, profile.Phone, profile.WebUrl, profile.Industry)
				if err != nil {
					return fmt.Errorf("error while staging company profiles: %w", err)
				}

				rowsModified += r.RowsAffected()
				rowsStaged++

				util.Logf(ctx, logging.Debug, "successfully staged company profile: %v", response.Request.Symbol)
			}

			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to stage company profiles: %w", err)
		}

		ret = StagingInfo{RowsModified: rowsModified, RowsStaged: rowsStaged}
		return nil
	}, bo, bon)

	return
}

func lookupCompanyProfilesToStage(ctx context.Context, jobRunId uint64, tx pgx.Tx) (ret []api.CompanyProfileResponse, err error) {
	ret = make([]api.CompanyProfileResponse, 0, 50_000) // should be big enough for a while

	rows, err := tx.Query(ctx, `SELECT symbol, data FROM src.company_profiles WHERE job_run_id = $1`, jobRunId)
	if err != nil {
		return nil, fmt.Errorf("failed to get source company profiles: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var d api.CompanyProfileResponse
		err := rows.Scan(&d.Request.Symbol, &d.Response)
		if err != nil {
			return nil, fmt.Errorf("failed to scan source company profile: %w", err)
		}
		ret = append(ret, d)
	}

	return ret, nil
}

func StageCandles52Wk(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, bo backoff.BackOff, bon backoff.Notify, candles api.CandlesResponse) (ret StagingInfo, err error) {
	ctx = util.WithLoggerValue(ctx, "action", "stage")

	err = backoff.RetryNotify(func() error {
		err := util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()

			a, ac, err := calcAffected52WkCandles(ctx, jobRunId, tx, candles)
			if err != nil {
				return err
			}
			util.Logf(ctx, logging.Debug, "successfully calculated %d affected 52wk candles for symbol %s", ac, candles.Request.Symbol)

			r, err := updateAffected52WkCandles(ctx, tx, jobRunId, a)
			if err != nil {
				return err
			}

			ret = StagingInfo{RowsModified: r, RowsStaged: ac}
			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to stage 52 week candles: %w", err)
		}

		return nil
	}, bo, bon)

	return
}

func updateAffected52WkCandles(ctx context.Context, tx pgx.Tx, jobRunId uint64, affected map[string][]time.Time) (int64, error) {
	var rowsModified int64
	var rowsAffected int64

	for symbol, timestamps := range affected {
		var symbolRowsModified int64
		ctx := util.WithLoggerValue(ctx, "symbol", symbol)
		for _, timestamp := range timestamps {
			modified, err := updateOneAffected52WkCandle(ctx, tx, jobRunId, symbol, timestamp)
			if err != nil {
				return 0, err
			}
			symbolRowsModified += modified
		}

		util.Logf(ctx, logging.Debug, fmt.Sprintf("successfully updated %d affected 52wk candles for symbol %s (%d rows modified)", len(timestamps), symbol, symbolRowsModified))

		rowsModified += symbolRowsModified
		rowsAffected += int64(len(timestamps))
	}

	util.Logf(ctx, logging.Debug, "successfully updated %d affected 52wk candles for %d symbols (%d rows modified)", rowsAffected, len(affected), rowsModified)
	return rowsModified, nil
}

func updateOneAffected52WkCandle(ctx context.Context, tx pgx.Tx, jobRunId uint64, symbol string, timestamp time.Time) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	r, err := tx.Exec(ctx, `
		INSERT INTO stage.candles_52wk 
			(job_run_id, symbol, timestamp, high_52wk, low_52wk, volume_52wk_avg, open, high, low, close, volume, created, modified) 
		SELECT 
			$1 as job_run_id, symbol, timestamp, high_52wk, low_52wk, volume_52wk_avg, open, high, low, close, volume, created, modified
		FROM stage.calculate_candles_52wk 
		WHERE 
			calculate_candles_52wk.symbol = $2
			AND calculate_candles_52wk.timestamp = $3
		ON CONFLICT (symbol, timestamp) 
		DO UPDATE 
			SET 
				job_run_id = excluded.job_run_id,
				high_52wk = excluded.high_52wk, 
				low_52wk = excluded.low_52wk, 
				volume_52wk_avg = excluded.volume_52wk_avg, 
				open = excluded.open, 
				high = excluded.high, 
				low = excluded.low,
				close = excluded.close, 
				volume = excluded.volume, 
				modified = excluded.modified,
				timestamp_52wk_count = excluded.timestamp_52wk_count
			WHERE 
				candles_52wk.high_52wk IS DISTINCT FROM excluded.high_52wk OR 
				candles_52wk.low_52wk IS DISTINCT FROM excluded.low_52wk OR 
				candles_52wk.volume_52wk_avg IS DISTINCT FROM excluded.volume_52wk_avg OR 
				candles_52wk.open IS DISTINCT FROM excluded.open OR 
				candles_52wk.high IS DISTINCT FROM excluded.high OR 
				candles_52wk.low IS DISTINCT FROM excluded.low OR
				candles_52wk.close IS DISTINCT FROM excluded.close OR 
				candles_52wk.volume IS DISTINCT FROM excluded.volume OR
				candles_52wk.timestamp_52wk_count IS DISTINCT FROM excluded.timestamp_52wk_count
		`, jobRunId, symbol, timestamp)

	if err != nil {
		return 0, fmt.Errorf("failed to update 52 week candle %v %v: %w", symbol, timestamp, err)
	}

	return r.RowsAffected(), nil
}

func calcAffected52WkCandles(ctx context.Context, jobRunId uint64, tx pgx.Tx, resp api.CandlesResponse) (map[string][]time.Time, int64, error) {
	var rowsAffected int64
	rs, err := tx.Query(ctx, `
		SELECT
			affected.symbol,
			affected.timestamp
		FROM stage.candles modified
		JOIN stage.candles affected
			ON modified.symbol = affected.symbol
			AND affected.timestamp BETWEEN modified.timestamp - INTERVAL '52 week' AND modified.timestamp
		WHERE
			modified.symbol = $1
			AND modified.job_run_id = $2
		GROUP BY
			affected.symbol,
			affected.timestamp
		`, resp.Request.Symbol, jobRunId)
	if err != nil {
		return nil, 0, fmt.Errorf("error while determining affected 52 week candles: %w", err)
	}
	defer rs.Close()

	var ret = map[string][]time.Time{}
	for rs.Next() {
		var symbol string
		var timestamp time.Time
		err := rs.Scan(&symbol, &timestamp)
		if err != nil {
			return nil, 0, fmt.Errorf("error while reading affected 52 week candles: %w", err)
		}

		rowsAffected++
		if _, ok := ret[symbol]; !ok {
			ret[symbol] = []time.Time{timestamp}
			continue
		}

		ret[symbol] = append(ret[symbol], timestamp)
	}

	return ret, rowsAffected, nil
}

type StagingInfo struct {
	RowsModified int64
	RowsStaged   int64
}

type LatestCandles map[api.Symbol]LatestCandleTime
type LatestCandleTime time.Time

func LookupLatestCandles(ctx context.Context, pool *pgxpool.Pool, bo backoff.BackOff, bon backoff.Notify) (LatestCandles, error) {
	var ret LatestCandles
	err := backoff.RetryNotify(func() error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		return util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			rows, err := pool.Query(ctx, `
				SELECT DISTINCT 
					candles.symbol, max(candles.timestamp) 
				FROM stage.candles 
				JOIN metadata.job_run
					ON candles.job_run_id = job_run.id
				WHERE job_run.success = TRUE
				GROUP BY candles.symbol`)
			if err != nil {
				return fmt.Errorf("failed to query latest stocks: %w", err)
			}
			defer rows.Close()

			ret = make(LatestCandles)
			for rows.Next() {
				var symbol api.Symbol
				var timestamp time.Time
				err := rows.Scan(&symbol, &timestamp)
				if err != nil {
					return fmt.Errorf("failed to parse latest stocks: %w", err)
				}
				ret[symbol] = LatestCandleTime(timestamp)
			}
			return nil
		})
	}, bo, bon)

	if err != nil {
		return nil, err
	}

	return ret, nil
}

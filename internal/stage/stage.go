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
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/gke"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"

	"github.com/ajjensen13/stocker/internal/extract"
	"github.com/ajjensen13/stocker/internal/transform"
	"github.com/ajjensen13/stocker/internal/util"
)

func Stocks(ctx context.Context, lg gke.Logger, pool *pgxpool.Pool, jobRunId uint64, bo backoff.BackOff, bon backoff.Notify) (ret StagingInfo, err error) {
	err = backoff.RetryNotify(func() error {
		var rowsStaged, rowsModified int64

		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		err := util.RunTx(ctx, pool, func(tx pgx.Tx) error {
			ess, err := querySrcStocks(ctx, jobRunId, tx)
			if err != nil {
				return err
			}

			stocks := transform.Stocks(ess)

			for _, stock := range stocks {
				lg.Debugf("staging stock: %v", stock.Symbol)

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
			}
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

func querySrcStocks(ctx context.Context, jobRunId uint64, tx pgx.Tx) (ret []finnhub.Stock, err error) {
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

func Candles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, bo backoff.BackOff, bon backoff.Notify, tz *time.Location) (ret StagingInfo, err error) {
	err = backoff.RetryNotify(func() error {
		var rowsStaged, rowsModified int64

		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		err := util.RunTx(ctx, pool, func(tx pgx.Tx) error {
			ecs, err := querySrcCandles(ctx, jobRunId, tx)
			if err != nil {
				return err
			}

			tcs, err := transform.StockCandles(ecs, tz)
			if err != nil {
				return err
			}

			for _, candles := range tcs {
				lg.Debugf("staging candles: %v", len(candles))

				for _, candle := range candles {
					lg.Debugf("staging candles: %v", candle.Symbol.String)

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

					r, err := tx.Exec(ctx, sql, jobRunId, candle.Symbol, candle.Timestamp, candle.Open, candle.High, candle.Low, candle.Close, candle.Volume)
					if err != nil {
						return fmt.Errorf("error while staging candles: %w", err)
					}

					rowsModified += r.RowsAffected()
					rowsStaged++
				}
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

func querySrcCandles(ctx context.Context, jobRunId uint64, tx pgx.Tx) (ret []extract.StockCandlesWithMetadata, err error) {
	rows, err := tx.Query(ctx, `SELECT symbol, data FROM src.candles WHERE job_run_id = $1`, jobRunId)
	if err != nil {
		return nil, fmt.Errorf("failed to get source candles: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var d extract.StockCandlesWithMetadata
		err := rows.Scan(&d.Symbol, &d.StockCandles)
		if err != nil {
			return nil, fmt.Errorf("failed to scan source candles: %w", err)
		}
		ret = append(ret, d)
	}

	return ret, nil
}

func CompanyProfiles(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, bo backoff.BackOff, bon backoff.Notify) (ret StagingInfo, err error) {
	err = backoff.RetryNotify(func() error {
		var rowsStaged, rowsModified int64

		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		err := util.RunTx(ctx, pool, func(tx pgx.Tx) error {
			profiles, err := querySrcCompanyProfiles(ctx, jobRunId, tx)
			if err != nil {
				return err
			}

			tcps, err := transform.CompanyProfiles(profiles)
			if err != nil {
				return err
			}

			for _, tcp := range tcps {
				lg.Debugf("staging company profile: %v", tcp.Symbol)

				sql := `
					INSERT INTO stage.company_profiles
						(job_run_id, symbol, exchange, country, currency, name, ipo, market_capitalization, shares_outstanding, logo, phone, web_url, industry, created, modified)
					VALUES
						($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) 
					ON CONFLICT 
						(symbol) 
					DO UPDATE 
						SET 
							job_run_id = excluded.job_run_id,
							exchange = excluded.exchange, 
							country = excluded.country, 
							currency = excluded.currency, 
							name = excluded.name, 
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
							company_profiles.ipo IS DISTINCT FROM excluded.ipo OR
							company_profiles.market_capitalization IS DISTINCT FROM excluded.market_capitalization OR
							company_profiles.shares_outstanding IS DISTINCT FROM excluded.shares_outstanding OR
							company_profiles.logo IS DISTINCT FROM excluded.logo OR
							company_profiles.phone IS DISTINCT FROM excluded.phone OR
							company_profiles.web_url IS DISTINCT FROM excluded.web_url OR
							company_profiles.industry IS DISTINCT FROM excluded.industry
`

				r, err := tx.Exec(ctx, sql, jobRunId, tcp.Symbol, tcp.Exchange, tcp.Country, tcp.Currency, tcp.Name, tcp.Ipo, tcp.MarketCapitalization, tcp.SharesOutstanding, tcp.Logo, tcp.Phone, tcp.WebUrl, tcp.Industry)
				if err != nil {
					return fmt.Errorf("error while staging company profiles: %w", err)
				}

				rowsModified += r.RowsAffected()
				rowsStaged++
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

func querySrcCompanyProfiles(ctx context.Context, jobRunId uint64, tx pgx.Tx) (ret []finnhub.CompanyProfile2, err error) {
	ret = make([]finnhub.CompanyProfile2, 0, 50_000)

	rows, err := tx.Query(ctx, `SELECT data FROM src.company_profiles WHERE job_run_id = $1`, jobRunId)
	if err != nil {
		return nil, fmt.Errorf("failed to get source company profiles: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var src finnhub.CompanyProfile2
		err := rows.Scan(&src)
		if err != nil {
			return nil, fmt.Errorf("failed to scan source company profile: %w", err)
		}
		ret = append(ret, src)
	}

	return ret, nil
}

func Candles52Wk(ctx context.Context, lg gke.Logger, jobRunId uint64, pool *pgxpool.Pool, symbol string, bo backoff.BackOff, bon backoff.Notify) (ret StagingInfo, err error) {
	err = backoff.RetryNotify(func() error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		err := util.RunTx(ctx, pool, func(tx pgx.Tx) error {
			a, ac, err := calcAffected(ctx, jobRunId, tx, symbol)
			if err != nil {
				return err
			}
			lg.Defaultf("successfully calculated %d affected 52wk candles for symbol %s", ac, symbol)

			r, err := updateAffected(ctx, lg, tx, jobRunId, a)
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

func updateAffected(ctx context.Context, lg gke.Logger, tx pgx.Tx, jobRunId uint64, affected map[string][]time.Time) (int64, error) {
	var rowsModified int64
	var rowsAffected int64

	for symbol, timestamps := range affected {
		var symbolRowsModified int64
		lg.Debugf("updated affected 52wk candles: %s", symbol)

		for _, timestamp := range timestamps {
			lg.Debugf("updated affected 52wk candles: %s %v", symbol, timestamp)

			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
				cancel()
				return 0, fmt.Errorf("failed to update 52 week candle %v %v: %w", symbol, timestamp, err)
			}

			symbolRowsModified += r.RowsAffected()
			cancel()
		}

		lg.Default(struct {
			Message    string
			Symbol     string
			Timestamps []time.Time
		}{
			Message:    fmt.Sprintf("successfully updated %d affected 52wk candles for symbol %s (%d rows modified)", len(timestamps), symbol, symbolRowsModified),
			Symbol:     symbol,
			Timestamps: timestamps,
		})

		rowsModified += symbolRowsModified
		rowsAffected += int64(len(timestamps))
	}

	lg.Defaultf("successfully updated %d affected 52wk candles for %d symbols (%d rows modified)", rowsAffected, len(affected), rowsModified)
	return rowsModified, nil
}

func calcAffected(ctx context.Context, jobRunId uint64, tx pgx.Tx, symbol string) (map[string][]time.Time, int64, error) {
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
		`, symbol, jobRunId)
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

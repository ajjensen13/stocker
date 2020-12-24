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

package src

import (
	"cloud.google.com/go/logging"
	"context"
	"fmt"
	"github.com/Finnhub-Stock-API/finnhub-go"
	"github.com/ajjensen13/stocker/internal/external"
	"github.com/ajjensen13/stocker/internal/util"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
)

func InsertStocks(ctx context.Context, pool *pgxpool.Pool, jobRunId uint64, ss []finnhub.Stock, bo backoff.BackOff, bon backoff.Notify) error {
	ctx = util.WithLoggerValue(ctx, "action", "load")
	return backoff.RetryNotify(func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		return util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			for _, stock := range ss {
				ctx := util.WithLoggerValue(ctx, "symbol", stock.Symbol)

				if stock.Symbol == "" {
					util.Logf(ctx, logging.Debug, "skipping stock due to missing symbol: %v", stock)
					continue
				}
				_, err := tx.Exec(ctx, `INSERT INTO src.stocks (job_run_id, symbol, data) VALUES ($1, $2, $3)`, jobRunId, stock.Symbol, stock)
				if err != nil {
					return fmt.Errorf("failed to load stock symbol %q: %w", stock.Symbol, err)
				}

				util.Logf(ctx, logging.Debug, "successfully inserted stock %q into src.stocks", stock.Symbol)
			}
			return nil
		})
	}, bo, bon)
}

func InsertCandles(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, ss external.StockCandlesWithMetadata, bo backoff.BackOff, bon backoff.Notify) error {
	ctx = util.WithLoggerValue(ctx, "action", "load")
	return backoff.RetryNotify(func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		return util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			_, err = tx.Exec(ctx, `INSERT INTO src.candles (job_run_id, symbol, "from", "to", data) VALUES ($1, $2, $3, $4, $5)`, jobRunId, ss.Symbol, ss.From, ss.To, ss.StockCandles)
			if err != nil {
				return fmt.Errorf("failed to load stock symbol %q: %w", ss.Symbol, err)
			}
			return nil
		})
	}, bo, bon)
}

func InsertCompanyProfile(ctx context.Context, jobRunId uint64, pool *pgxpool.Pool, cp finnhub.CompanyProfile2, bo backoff.BackOff, bon backoff.Notify) error {
	ctx = util.WithLoggerValue(ctx, "action", "load")
	return backoff.RetryNotify(func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		return util.RunTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			_, err = tx.Exec(ctx, `INSERT INTO src.company_profiles (job_run_id, ticker, data) VALUES ($1, $2, $3)`, jobRunId, cp.Ticker, cp)
			if err != nil {
				return fmt.Errorf("failed to load %s company profile %q: %w", cp.Exchange, cp.Ticker, err)
			}
			return nil
		})
	}, bo, bon)
}

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
package cmd

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"time"

	"github.com/spf13/cobra"

	"github.com/ajjensen13/gke"

	"github.com/ajjensen13/stocker/internal/load"
	"github.com/ajjensen13/stocker/internal/model"
	"github.com/ajjensen13/stocker/internal/transform"
)

const (
	dbSecretName  = "stocker-db-secret.json"
	appConfigName = "stocker-config-cm.json"
	apiSecretName = "stocker-api-secret.json"
)

type appConfig struct {
	LookBackDays   int       `json:"lookback_days"`
	Exchange       string    `json:"exchange"`
	Resolution     string    `json:"resolution"`
	OverrideDate   time.Time `json:"override_date"`
	DataSourceName string    `json:"data_source_name"`
}

type appSecrets struct {
	ApiKey string `json:"api_key"`
}

// etlCmd represents the etl command
var etlCmd = &cobra.Command{
	Use:   "etl",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		lg, cleanup, err := gke.NewLogger(context.Background())
		if err != nil {
			panic(err)
		}
		defer cleanup()

		gke.LogEnv(lg)
		gke.LogMetadata(lg)

		tx, cleanup, err := openTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to setup database transaction: %w", err)))
		}
		defer cleanup()

		ctx := context.Background()
		ess, err := extractStocks(ctx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to extract stocks: %w", err)))
		}

		tss := make([]model.Stock, 0, len(ess))
		for _, es := range ess {
			if es.Symbol == "" {
				lg.Warningf("skipping stock without symbol: %v", es)
				continue
			}
			tss = append(tss, transform.Stock(es))
		}

		err = load.Stocks(ctx, tx, tss)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to load stocks: %w", err)))
		}
		lg.Defaultf("extracted & loaded %d stocks", len(tss))

		throttler := time.NewTicker(time.Second)
		defer throttler.Stop()

		<-throttler.C
		for _, es := range ess {
			select {
			case <-ctx.Done():
				panic(lg.WarningErr(fmt.Errorf("aborting candle extraction %q: %w", es.Symbol, ctx.Err())))
			case <-throttler.C:
				ec, err := extractCandles(ctx, es)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to extract stock candles %q: %w", es.Symbol, err)))
				}

				tcs, err := transform.Candles(es, ec)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to transform stock candles %q: %w", es.Symbol, err)))
				}

				err = load.Candles(ctx, tx, tcs)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to load stock candles %q: %w", es.Symbol, err)))
				}

				lg.Defaultf("extracted & loaded %d stock candles: %s", len(tcs), es.Symbol)
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("error while committing database transaction: %w", err)))
		}

		lg.Defaultf("committed database transaction")
	},
}

func init() {
	rootCmd.AddCommand(etlCmd)
}

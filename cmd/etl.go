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
	"github.com/Finnhub-Stock-API/finnhub-go"
	"time"

	"github.com/spf13/cobra"

	"github.com/ajjensen13/gke"

	"github.com/ajjensen13/stocker/internal/load"
	"github.com/ajjensen13/stocker/internal/model"
	"github.com/ajjensen13/stocker/internal/stage"
	"github.com/ajjensen13/stocker/internal/transform"
)

const (
	dbSecretName  = "stocker-db-secret.json"
	appConfigName = "stocker-config-cm.json"
	apiSecretName = "stocker-api-secret.json"
)

type appConfig struct {
	Exchange           string    `json:"exchange"`
	Resolution         string    `json:"resolution"`
	StartDate          time.Time `json:"start_date"`
	EndDate            time.Time `json:"end_date"`
	DataSourceName     string    `json:"data_source_name"`
	Timezone           string    `json:"timezone"`
	MigrationSourceURL string    `json:"migration_source_url"`
}

type appSecrets struct {
	ApiKey string `json:"api_key"`
}

// etlCmd represents the etl command
var etlCmd = &cobra.Command{
	Use:   "etl",
	Short: "runs a stocker etl",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		lg, cleanup := logger()
		defer cleanup()

		tx, cleanup, err := openTx(context.Background())
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to setup database transaction: %w", err)))
		}
		defer cleanup()

		ctx := context.Background()

		ess, err := requestStocks(ctx, lg)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to retrieve stocks from finnhub: %w", err)))
		}

		ess, err = skipAndLimit(cmd, lg, ess)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to reduce result set using skip and limit: %w", err)))
		}

		latest, err := queryMostRecentCandles(ctx, lg, tx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to get latest stocks: %w", err)))
		}
		lg.Default(gke.NewFmtMsgData("extracted %d existing stocks from database", len(latest)))

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
			panic(lg.ErrorErr(fmt.Errorf("failed to load stocks into database: %w", err)))
		}
		lg.Default(gke.NewFmtMsgData("extracted & loaded %d stocks", len(tss)))

		throttler := time.NewTicker(time.Second)
		defer throttler.Stop()

		tz, err := timezone()
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to get timezone: %w", err)))
		}
		lg.Default(gke.NewFmtMsgData("using %v timezone to store data", tz))

		<-throttler.C
		for _, es := range ess {
			select {
			case <-ctx.Done():
				panic(lg.WarningErr(fmt.Errorf("aborting candle request %q from finnhub: %w", es.Symbol, ctx.Err())))
			case <-throttler.C:
				ec, err := requestCandles(ctx, lg, es, latest)
				if err != nil {
					_ = lg.ErrorErr(fmt.Errorf("failed to retrieve stock candles %q from finnhub: %w", es.Symbol, err))
					_ = lg.Infof("stock candles %q will be skipped due to error", es.Symbol)
					continue
				}

				tcs, err := transform.Candles(es, ec, tz)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to transform stock candles %q: %w", es.Symbol, err)))
				}

				err = load.Candles(ctx, tx, tcs)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to load stock candles %q into database: %w", es.Symbol, err)))
				}

				lg.Defaultf("requested & loaded %d stock candles from finnhub into database: %s", len(tcs), es.Symbol)
			}
		}

		for _, es := range ess {
			select {
			case <-ctx.Done():
				panic(lg.WarningErr(fmt.Errorf("aborting company profile request %q from finnhub: %w", es.Symbol, ctx.Err())))
			case <-throttler.C:
				ecp, err := requestCompanyProfile(ctx, lg, es)
				if err != nil {
					_ = lg.ErrorErr(fmt.Errorf("failed to retrieve company profile %q from finnhub: %w", es.Symbol, err))
					_ = lg.Infof("company profile %q will be skipped due to error", es.Symbol)
					continue
				}

				tcp, err := transform.CompanyProfile(ecp)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to transform company profile %q: %w", es.Symbol, err)))
				}

				err = load.CompanyProfile(ctx, tx, tcp)
				if err != nil {
					panic(lg.ErrorErr(fmt.Errorf("failed to load company profile %q into database: %w", es.Symbol, err)))
				}

				lg.Defaultf("requested & loaded company profile from finnhub into database: %q", es.Symbol)
			}
		}

		si, err := stage.Stocks(ctx, tx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to stage stocks: %w", err)))
		}
		lg.Defaultf("successfully staged %d stocks (previous latest modification: %v)", si.RowsAffected, si.PreviousLatestModification)

		si, err = stage.CompanyProfile(ctx, tx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to stage company profiles: %w", err)))
		}
		lg.Defaultf("successfully staged %d company profiles (previous latest modification: %v)", si.RowsAffected, si.PreviousLatestModification)

		si, err = stage.Candles(ctx, tx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to stage candles: %w", err)))
		}
		lg.Defaultf("successfully staged %d candles (previous latest modification: %v)", si.RowsAffected, si.PreviousLatestModification)

		si, err = stage.Candles52Wk(ctx, lg, tx, si.PreviousLatestModification)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("failed to stage candles: %w", err)))
		}
		lg.Defaultf("successfully staged %d 52wk candles (previous latest modification: %v)", si.RowsAffected, si.PreviousLatestModification)

		err = tx.Commit(ctx)
		if err != nil {
			panic(lg.ErrorErr(fmt.Errorf("error while committing database transaction: %w", err)))
		}

		lg.Defaultf("committed database transaction")
	},
}

func skipAndLimit(cmd *cobra.Command, lg gke.Logger, ess []finnhub.Stock) ([]finnhub.Stock, error) {
	reqS, err := cmd.Flags().GetInt("skip")
	if err != nil {
		return nil, fmt.Errorf("failed to get skip flag: %w", err)
	}
	var actS = 0
	if reqS > -1 {
		actS = reqS
		maxS := len(ess)
		if reqS > maxS {
			actS = reqS
		}
		lg.Defaultf("skipping %d entries (skip %d requested, %d entries found)", actS, reqS, maxS)
		ess = ess[actS:]
	}

	reqL, err := cmd.Flags().GetInt("limit")
	if err != nil {
		return nil, fmt.Errorf("failed to get limit flag: %w", err)
	}
	var actL = 0
	if reqL > -1 {
		actL = reqL
		maxL := len(ess)
		if reqL > maxL {
			actL = reqL
		}
		lg.Defaultf("limiting to %d entries (limit %d requested, %d entries remain after skipping %d)", actL, reqL, maxL, actS)
		ess = ess[:actL]
	}
	return ess, nil
}

func init() {
	rootCmd.AddCommand(etlCmd)
	etlCmd.Flags().IntP("skip", "s", -1, "number of stocks to skip")
	etlCmd.Flags().IntP("limit", "l", -1, "maximum number of stocks to update")
}

type latestStock struct {
	symbol    string
	timestamp time.Time
}

type candleConfig struct {
	resolution string
	startDate  time.Time
	endDate    time.Time
}

type latestStocks map[string]time.Time

type apiAuthContext context.Context

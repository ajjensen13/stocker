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
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ajjensen13/config"
	"github.com/ajjensen13/gke"

	"github.com/ajjensen13/stocker/internal/extract"

	_ "github.com/lib/pq"
)

const (
	dbSecretName  = "stocker-db-secret.json"
	appConfigName = "stocker-config-cm.json"
	apiSecretName = "stocker-api-secret.json"
)

func provideDbSecrets() (*url.Userinfo, error) {
	ui, err := config.Userinfo(dbSecretName)
	if err != nil {
		return nil, err
	}
	return ui, nil
}

type appConfig struct {
	LookBackDays   int       `json:"lookback_days"`
	Exchange       string    `json:"exchange"`
	OverrideDate   time.Time `json:"override_date"`
	DataSourceName string    `json:"data_source_name"`
}

func provideAppConfig() (*appConfig, error) {
	var result appConfig
	err := config.InterfaceJson(appConfigName, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

type appSecrets struct {
	ApiKey string `json:"api_key"`
}

func provideAppSecrets() (*appSecrets, error) {
	var result appSecrets
	err := config.InterfaceJson(apiSecretName, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// etlCmd represents the extract command
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

		throttler := time.NewTicker(time.Second)
		defer throttler.Stop()

		stocks := make(chan extract.Stock)
		gke.Do(func(ctx context.Context) error {
			defer close(stocks)

			cfg, err := provideAppConfig()
			if err != nil {
				return lg.ErrorErr(fmt.Errorf("failed to load app config: %w", err))
			}

			secrets, err := provideAppSecrets()
			if err != nil {
				return lg.ErrorErr(fmt.Errorf("failed to load app secrets: %w", err))
			}

			select {
			case <-throttler.C:
				return extract.ExtractStocks(ctx, stocks, secrets.ApiKey, cfg.Exchange)
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		candles := make(chan extract.StockCandles)
		gke.Do(func(ctx context.Context) error {
			cfg, err := provideAppConfig()
			if err != nil {
				return lg.ErrorErr(fmt.Errorf("failed to load app config: %w", err))
			}

			dsn, err := url.Parse(cfg.DataSourceName)
			if err != nil {
				return lg.ErrorErr(fmt.Errorf("failed to parse dsn: %w", err))
			}

			user, err := provideDbSecrets()
			if err != nil {
				return lg.ErrorErr(fmt.Errorf("failed to load db secrets: %w", err))
			}
			dsn.User = user

			db, err := sql.Open("postgres", dsn.String())
			if err != nil {
				return lg.ErrorErr(fmt.Errorf("failed to open db connection: %w", err))
			}
			defer db.Close()

			err = db.Ping()
			if err != nil {
				return lg.ErrorErr(fmt.Errorf("failed to ping db: %w", err))
			}

			for candle := range candles {
				err = json.NewEncoder(os.Stdout).Encode(&candle)
				if err != nil {
					_ = lg.ErrorErr(fmt.Errorf("failed to format response data: %w", err))
					continue
				}
			}
			return nil
		})

		gke.Do(func(ctx context.Context) error {
			cfg, err := provideAppConfig()
			if err != nil {
				return lg.ErrorErr(fmt.Errorf("failed to load app config: %w", err))
			}

			secrets, err := provideAppSecrets()
			if err != nil {
				return lg.ErrorErr(err)
			}

			var to time.Time
			if cfg.OverrideDate.IsZero() {
				now := time.Now()
				to = time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.Local)
			} else {
				to = cfg.OverrideDate
			}

			from := to.AddDate(0, 0, -cfg.LookBackDays)

			for stock := range stocks {
				select {
				case <-throttler.C:
					err := extract.ExtractStockCandles(ctx, candles, secrets.ApiKey, stock, "D", from, to)
					if err != nil {
						_ = lg.WarningErr(fmt.Errorf("failed to extract candle data: %w", err))
						continue
					}
				case <-ctx.Done():
					break
				}
			}
			return nil
		})

		cleanupCtx := gke.AfterAliveContext(time.Second * 10)
		<-cleanupCtx.Done()
	},
}

func init() {
	rootCmd.AddCommand(etlCmd)
}

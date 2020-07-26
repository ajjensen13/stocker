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
	"errors"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use: "migrate",
}

var upCmd = &cobra.Command{
	Use: "up",
	Run: func(cmd *cobra.Command, args []string) {
		lg, cleanup := logger()
		defer cleanup()

		m, err := migrator(lg)
		if err != nil {
			panic(lg.ErrorErr(err))
		}

		err = m.Up()
		switch {
		case errors.Is(err, migrate.ErrNoChange):
			lg.Defaultf("database is already migrated fully up")
		case err != nil:
			panic(lg.ErrorErr(err))
		}
	},
}

var downCmd = &cobra.Command{
	Use: "down",
	Run: func(cmd *cobra.Command, args []string) {
		lg, cleanup := logger()
		defer cleanup()

		m, err := migrator(lg)
		if err != nil {
			panic(lg.ErrorErr(err))
		}
		defer cleanup()

		err = m.Down()
		switch {
		case errors.Is(err, migrate.ErrNoChange):
			lg.Defaultf("database is already migrated fully down")
		case err != nil:
			panic(lg.ErrorErr(err))
		}
	},
}

func init() {
	migrateCmd.AddCommand(upCmd)
	migrateCmd.AddCommand(downCmd)
	rootCmd.AddCommand(migrateCmd)
}

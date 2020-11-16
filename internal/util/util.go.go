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

package util

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"time"
)

const (
	ShortReqTimeout = 30 * time.Second
	MedReqTimeout   = 5 * time.Minute
	LongReqTimeout  = 12 * time.Hour
)

func WrapWithSavePoint(ctx context.Context, tx pgx.Tx, op backoff.Operation, sp string) backoff.Operation {
	return func() (err error) {
		tx, err = tx.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to create pseudo-transaction %s: %w", sp, err)
		}

		err = op()
		if err != nil {
			rbErr := tx.Rollback(ctx)
			if rbErr != nil {
				panic(rbErr)
			}
			return err
		}

		err = tx.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to commit pseudo-transaction %s: %w", sp, err)
		}

		return nil
	}
}

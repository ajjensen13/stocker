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
	LongReqTimeout  = 60 * time.Minute
)

func WrapWithSavePoint(ctx context.Context, tx pgx.Tx, op backoff.Operation, sp string) backoff.Operation {
	return func() (err error) {
		_, err = tx.Exec(ctx, `SAVEPOINT `+sp)
		if err != nil {
			err = fmt.Errorf("failed to create savepoint %s: %w", sp, err)
			return
		}

		defer func() {
			if err == nil {
				_, err = tx.Exec(ctx, `RELEASE SAVEPOINT `+sp)
				if err != nil {
					err = fmt.Errorf("failed to release savepoint %s: %w", sp, err)
				}
				return
			}
			_, err2 := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT `+sp)
			if err2 != nil {
				err = fmt.Errorf("failed to rollback to savepoint %s: %v. %w", sp, err2, err)
			}
		}()

		err = op()
		return
	}
}

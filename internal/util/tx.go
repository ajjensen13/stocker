package util

import (
	"cloud.google.com/go/logging"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync/atomic"
)

var nextTxId uint32

func RunTx(ctx context.Context, pool *pgxpool.Pool, f func(ctx context.Context, tx pgx.Tx) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to aquire connection: %w", err)
	}

	pid := conn.Conn().PgConn().PID()
	ctx = WithLoggerValue(ctx, "db_conn_pid", fmt.Sprintf("pid_%d", pid))
	Logf(ctx, logging.Debug, "acquired database connection [%d]", pid)

	defer func() {
		conn.Release()
		Logf(ctx, logging.Debug, "released database connection [%d]", pid)
	}()

	txid := atomic.AddUint32(&nextTxId, 1)
	ctx = WithLoggerValue(ctx, "db_conn_tx_id", fmt.Sprintf("tx_%d", txid))
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to create transaction [%d/%d]: %w", pid, txid, err)
	}
	Logf(ctx, logging.Debug, "started new database transaction [%d -> %d]", pid, txid)

	err = f(ctx, tx)
	if err != nil {
		Logf(ctx, logging.Debug, "rolling back database transaction [%d -> %d]", pid, txid)
		errRollback := tx.Rollback(ctx)
		if errRollback != nil {
			Logf(ctx, logging.Warning, "failed to rollback database transaction [%d -> %d]: %v", pid, txid, errRollback)
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit database transaction [%d -> %d]: %v", pid, txid, err)
	}

	Logf(ctx, logging.Debug, "successfully committed database transaction [%d -> %d]", pid, txid)
	return nil
}

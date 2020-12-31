package util

import (
	"cloud.google.com/go/logging"
	"context"
	"fmt"
	"github.com/ajjensen13/gke"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync/atomic"
)

type contextKey string

const (
	loggerContextKey contextKey = "logger"
	extraContextKey  contextKey = "extra"
)

func WithLoggerValue(ctx context.Context, key string, val interface{}) context.Context {
	var nm map[string]interface{}
	p := ctx.Value(extraContextKey)
	if p != nil {
		pm := p.(map[string]interface{})
		nm = make(map[string]interface{}, len(pm)+1)
		for k, v := range pm {
			nm[k] = v
		}
	} else {
		nm = map[string]interface{}{}
	}

	nm[key] = val
	return context.WithValue(ctx, extraContextKey, nm)
}

func WithLogger(ctx context.Context, lg gke.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey, lg)
}

type logPayload struct {
	Message string
	Values  map[string]interface{}
}

func (l logPayload) String() string {
	return l.Message
}

func Logf(ctx context.Context, severity logging.Severity, format string, argv ...interface{}) {
	log(ctx, severity, newLogPayload(ctx, fmt.Sprintf(format, argv...)))
}

func log(ctx context.Context, severity logging.Severity, payload logPayload) {
	entry := logging.Entry{Severity: severity, Payload: payload}
	gke.SetupSourceLocation(&entry, 2)
	ctx.Value(loggerContextKey).(gke.Logger).Log(entry)
}

func newLogPayload(ctx context.Context, msg string) logPayload {
	ret := logPayload{Message: msg}
	if v := ctx.Value(extraContextKey); v != nil {
		ret.Values = v.(map[string]interface{})
	}
	return ret
}

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

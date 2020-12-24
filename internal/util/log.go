package util

import (
	"cloud.google.com/go/logging"
	"context"
	"fmt"
	"github.com/ajjensen13/gke"
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

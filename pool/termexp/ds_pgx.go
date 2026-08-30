package termexp

import (
	"log/slog"
	"reflect"
)

// Adapter
type daoPgx struct {
	log *slog.Logger
}

func newDaoPgx(l *slog.Logger) *daoPgx {
	name := slog.String("name", reflect.TypeFor[daoPgx]().Name())
	return &daoPgx{l.With(name)}
}

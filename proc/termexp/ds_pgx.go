package termexp

import (
	"log/slog"
	"reflect"

	"orglang/go-engine/lib/db"
)

// Adapter
type daoPgx struct {
	log *slog.Logger
}

// for compilation purposes
func newRepo() Repo {
	return new(daoPgx)
}

func newDaoPgx(l *slog.Logger) *daoPgx {
	name := slog.String("name", reflect.TypeFor[daoPgx]().Name())
	return &daoPgx{l.With(name)}
}

func (dao *daoPgx) Insert(uow db.UoW, rec ExpRec) error {
	return nil
}

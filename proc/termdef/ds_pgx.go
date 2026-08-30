package termdef

import (
	"log/slog"
	"orglang/go-engine/lib/db"
	"reflect"
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

func (dao *daoPgx) InsertProc(db.UoW, DefRec) error {
	return nil
}

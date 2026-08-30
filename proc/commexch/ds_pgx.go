package commexch

import (
	"log/slog"
	"orglang/go-engine/adt/commsem"
	"orglang/go-engine/adt/uniqsym"
	"orglang/go-engine/lib/db"
	"reflect"
)

type daoPgx struct {
	log *slog.Logger
}

func newDaoPgx(log *slog.Logger) *daoPgx {
	name := slog.String("name", reflect.TypeFor[daoPgx]().Name())
	return &daoPgx{log.With(name)}
}

// for compilation purposes
func newRepo() Repo {
	return new(daoPgx)
}

func (dao *daoPgx) AddRec(db.UoW, ExchRec) error {
	panic("unimplemented")
}

func (dao *daoPgx) GetRefsByQNs(db.UoW, []uniqsym.ADT) (map[uniqsym.ADT]commsem.SemRef, error) {
	panic("unimplemented")
}

func (dao *daoPgx) GetSnapByQry(db.UoW, ExchQry) (ExchSnap, error) {
	panic("unimplemented")
}

func (dao *daoPgx) Modifyec(db.UoW, ExchMod) error {
	panic("unimplemented")
}

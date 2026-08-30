package compsem

import (
	"fmt"
	"log/slog"
	"reflect"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"
)

type daoPgx struct {
	qb  queryBuilder
	log *slog.Logger
}

func NewDaoPgx(table string) func(log *slog.Logger) *daoPgx {
	return func(log *slog.Logger) *daoPgx {
		name := slog.String("name", reflect.TypeFor[daoPgx]().Name())
		return &daoPgx{newSQLBuilder(table), log.With(name)}
	}
}

// for compilation purposes
func newRepo() Repo {
	return new(daoPgx)
}

func (dao *daoPgx) TouchRef(uow db.UoW, ref SemRef) error {
	dto := DataFromRef(ref)
	refAttr := slog.Any("ref", ref)
	sql, args := dao.qb.updateRef(dto)
	ct, execErr := uow.Pgx.Exec(uow.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", refAttr, slog.String("sql", sql))
		return execErr
	}
	if ct.RowsAffected() == 0 {
		dao.log.Error("touching failed", refAttr)
		return errConcurrentModification(ref)
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "touching succeed", refAttr)
	return nil
}

func errConcurrentModification(got SemRef) error {
	return fmt.Errorf("concurrent modification: %v", got)
}

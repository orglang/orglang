package implsem

import (
	"log/slog"
	"reflect"

	"orglang/go-engine/lib/db"
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

func (dao *daoPgx) AddRec(uow db.UoW, rec SemRec) error {
	recAttr := slog.Any("rec", rec)
	dto, convErr := DataFromRec(rec)
	if convErr != nil {
		dao.log.Error("model conversion failed", recAttr)
		return convErr
	}
	sql, args := dao.qb.insertRec(dto)
	_, execErr := uow.Pgx.Exec(uow.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", recAttr, slog.String("sql", sql))
		return execErr
	}
	return nil
}

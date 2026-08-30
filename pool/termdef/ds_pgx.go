package termdef

import (
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/uniqsym"
)

type daoPgx struct {
	qb  queryBuilder
	log *slog.Logger
}

func newDaoPgx(qb queryBuilder, log *slog.Logger) *daoPgx {
	name := slog.String("name", reflect.TypeFor[daoPgx]().Name())
	return &daoPgx{qb, log.With(name)}
}

// for compilation purposes
func newRepo() Repo {
	return new(daoPgx)
}

func (dao *daoPgx) AddRec(uow db.UoW, rec DefRec) error {
	refAttr := slog.Any("ref", rec.TermRef)
	dto, convErr := DataFromDecRec(rec)
	if convErr != nil {
		dao.log.Error("model conversion failed", refAttr)
		return convErr
	}
	sql, args := dao.qb.insertRec(dto)
	_, execErr := uow.Pgx.Exec(uow.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", refAttr)
		return execErr
	}
	return nil
}

func (dao *daoPgx) GetRecByQN(uow db.UoW, qn uniqsym.ADT) (DefRec, error) {
	qnAttr := slog.Any("qn", qn)
	sql, args := dao.qb.selectRecByQN(uniqsym.ConvertToString(qn))
	rows, execErr := uow.Pgx.Query(uow.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", qnAttr)
		return DefRec{}, execErr
	}
	dto, scanErr := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[defRecDS])
	if scanErr != nil {
		dao.log.Error("rows scanning failed", qnAttr)
		return DefRec{}, scanErr
	}
	rec, convErr := DataToDecRec(dto)
	if convErr != nil {
		dao.log.Error("model conversion failed", qnAttr)
		return DefRec{}, convErr
	}
	return rec, nil
}

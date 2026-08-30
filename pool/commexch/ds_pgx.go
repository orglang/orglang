package commexch

import (
	"log/slog"
	"reflect"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/commsem"
	"orglang/go-engine/adt/uniqsym"

	"github.com/jackc/pgx/v5"
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

func (dao *daoPgx) AddRec(ouw db.UoW, rec ExchRec) error {
	dto := DataFromRec(rec)
	refAttr := slog.Any("ref", rec.CommRef)
	sql, args := dao.qb.insertRec(dto)
	_, execErr := ouw.Pgx.Exec(ouw.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", refAttr, slog.String("sql", sql))
		return execErr
	}
	dao.log.Log(ouw.Ctx, lf.LevelTrace, "addition succeed", slog.Any("dto", dto))
	return nil
}

func (dao *daoPgx) ModifyRec(uow db.UoW, mod ExchMod) error {
	if mod.OffsetNr == nil {
		return nil
	}
	dto := DataFromMod(mod)
	refAttr := slog.Any("ref", mod.CommRef)
	sql, args := dao.qb.updateRec(dto)
	_, execErr := uow.Pgx.Exec(uow.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", refAttr, slog.String("sql", sql))
		return execErr
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "modification succeed", slog.Any("dto", dto))
	return nil
}

func (dao *daoPgx) GetRefsByQNs(uow db.UoW, qns []uniqsym.ADT) (map[uniqsym.ADT]commsem.SemRef, error) {
	panic("unimplemented")
}

func (dao *daoPgx) GetSnapByQry(ds db.UoW, qry ExchQry) (ExchSnap, error) {
	refAttr := slog.Any("ref", qry.CommRef)
	qryDTO := DataFromQry(qry)
	dao.log.Log(ds.Ctx, lf.LevelTrace, "getting started", slog.Any("qry", qryDTO))
	sql, args := dao.qb.selectSnap(qryDTO)
	rows, execErr := ds.Pgx.Query(ds.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", refAttr, slog.String("sql", sql))
		return ExchSnap{}, execErr
	}
	defer rows.Close()
	snapDTO, scanErr := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[exchSnapDS])
	if scanErr != nil {
		dao.log.Error("rows scanning failed", refAttr)
		return ExchSnap{}, scanErr
	}
	dao.log.Log(ds.Ctx, lf.LevelTrace, "getting succeed", slog.Any("dto", snapDTO))
	snap, convErr := DataToSnap(snapDTO)
	if convErr != nil {
		dao.log.Error("model conversion failed", refAttr)
		return ExchSnap{}, convErr
	}
	return snap, nil
}

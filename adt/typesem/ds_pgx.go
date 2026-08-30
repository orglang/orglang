package typesem

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/uniqsym"
)

type daoPgx struct {
	qb  queryBuilder
	log *slog.Logger
}

func NewDaoPgx(typeTable, descTable string) func(log *slog.Logger) *daoPgx {
	return func(log *slog.Logger) *daoPgx {
		name := slog.String("name", reflect.TypeFor[daoPgx]().Name())
		return &daoPgx{newSQLBuilder(typeTable, descTable), log.With(name)}
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

func (dao *daoPgx) GetRefsByQNs(uow db.UoW, typeQNs []uniqsym.ADT) (_ map[uniqsym.ADT]SemRef, err error) {
	dao.log.Log(uow.Ctx, lf.LevelTrace, "getting started", slog.Any("qns", typeQNs))
	if len(typeQNs) == 0 {
		return map[uniqsym.ADT]SemRef{}, nil
	}
	batch := pgx.Batch{}
	for _, typeQN := range typeQNs {
		sql := dao.qb.selectRefByQN()
		batch.Queue(sql, uniqsym.ConvertToString(typeQN))
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	dtos := make(map[uniqsym.ADT]SemRefDS, len(typeQNs))
	for _, typeQN := range typeQNs {
		qnAttr := slog.Any("qn", typeQN)
		rows, readErr := br.Query()
		if readErr != nil {
			dao.log.Error("query execution failed", qnAttr)
			return nil, readErr
		}
		dto, scanErr := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[SemRefDS])
		if scanErr != nil {
			dao.log.Error("row scanning failed", qnAttr)
			return nil, scanErr
		}
		dtos[typeQN] = dto
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "getting succeed", slog.Any("dtos", dtos))
	return DataToRefMap(dtos)
}

func errConcurrentModification(got SemRef) error {
	return fmt.Errorf("concurrent modification: %v", got)
}

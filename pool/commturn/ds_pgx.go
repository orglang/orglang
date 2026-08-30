package commturn

import (
	"errors"
	"log/slog"
	"reflect"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"

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

func (dao *daoPgx) AddRec(db.UoW, TurnRec) error {
	panic("unimplemented")
}

func (dao *daoPgx) AddRecs(uow db.UoW, recs []TurnRec) (err error) {
	dao.log.Log(uow.Ctx, lf.LevelTrace, "insertion started", slog.Any("recs", recs))
	batch := pgx.Batch{}
	for _, rec := range recs {
		dto := DataFromStepRec(rec)
		sql, args := dao.qb.insertRec(dto)
		batch.Queue(sql, args...)
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	for _, rec := range recs {
		_, readErr := br.Exec()
		if readErr != nil {
			dao.log.Error("query execution failed", slog.Any("rec", rec))
			return readErr
		}
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "insertion succeed")
	return nil
}

package termdec

import (
	"errors"
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/identity"
	"orglang/go-engine/adt/termsem"
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

func (dao *daoPgx) AddRec(uow db.UoW, rec DecRec) error {
	refAttr := slog.Any("ref", rec.TermRef)
	dto, err := DataFromDecRec(rec)
	if err != nil {
		dao.log.Error("model conversion failed", refAttr)
		return err
	}
	sql, args := dao.qb.insertRec(dto)
	_, err = uow.Pgx.Exec(uow.Ctx, sql, args...)
	if err != nil {
		dao.log.Error("query execution failed", refAttr, slog.String("sql", sql))
		return err
	}
	return nil
}

func (dao *daoPgx) GetSnap(uow db.UoW, ref termsem.SemRef) (DecSnap, error) {
	refAttr := slog.Any("ref", ref)
	rows, err := uow.Pgx.Query(uow.Ctx, selectByRef, ref.TermID.String())
	if err != nil {
		dao.log.Error("query execution failed", refAttr)
		return DecSnap{}, err
	}
	defer rows.Close()
	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[decSnapDS])
	if err != nil {
		dao.log.Error("row scanning failed", refAttr)
		return DecSnap{}, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entitiy selection succeed", slog.Any("dto", dto))
	return DataToDecSnap(dto)
}

func (dao *daoPgx) SelectEnv(uow db.UoW, ids []identity.ADT) (map[identity.ADT]DecRec, error) {
	decs, err := dao.GetRecs(uow, ids)
	if err != nil {
		return nil, err
	}
	env := make(map[identity.ADT]DecRec, len(decs))
	for _, dec := range decs {
		env[dec.TermRef.TermID] = dec
	}
	return env, nil
}

func (dao *daoPgx) GetRecs(uow db.UoW, ids []identity.ADT) (_ []DecRec, err error) {
	if len(ids) == 0 {
		return []DecRec{}, nil
	}
	batch := pgx.Batch{}
	for _, rid := range ids {
		if rid.IsEmpty() {
			return nil, identity.ErrEmpty
		}
		batch.Queue(selectByRef, rid.String())
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	var dtos []decRecDS
	for _, rid := range ids {
		rows, err := br.Query()
		if err != nil {
			dao.log.Error("query execution failed", slog.Any("id", rid), slog.String("q", selectByRef))
		}
		dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[decRecDS])
		if err != nil {
			dao.log.Error("row scanning failed", slog.Any("id", rid))
		}
		dtos = append(dtos, dto)
	}
	if err != nil {
		return nil, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "selection succeed", slog.Any("dtos", dtos))
	return DataToDecRecs(dtos)
}

func (dao *daoPgx) GetRefs(uow db.UoW) ([]termsem.SemRef, error) {
	query := `
		select
			desc_id, rev, title
		from dec_roots`
	rows, err := uow.Pgx.Query(uow.Ctx, query)
	if err != nil {
		dao.log.Error("query execution failed")
		return nil, err
	}
	defer rows.Close()
	dtos, err := pgx.CollectRows(rows, pgx.RowToStructByName[termsem.SemRefDS])
	if err != nil {
		dao.log.Error("rows scanning failed")
		return nil, err
	}
	return termsem.DataToRefs(dtos)
}

const (
	selectByRef = `
		select
			pd.desc_id,
			ds.desc_rn,
			pd.liab_var,
			pd.asset_vars
		from proc_term_decs pd
		left join desc_sems ds
			on ds.desc_id = pd.desc_id
		where pd.desc_id = $1`
)

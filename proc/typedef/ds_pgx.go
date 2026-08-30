package typedef

import (
	"errors"
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/identity"
	"orglang/go-engine/adt/typesem"
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
	idAttr := slog.Any("typeID", rec.TypeRef.TypeID)
	dao.log.Log(uow.Ctx, lf.LevelTrace, "addition started", idAttr)
	dto, err := DataFromDefRec(rec)
	if err != nil {
		dao.log.Error("model conversion failed", idAttr)
		return err
	}
	sql, args := dao.qb.insertRec(dto)
	_, err = uow.Pgx.Exec(uow.Ctx, sql, args...)
	if err != nil {
		dao.log.Error("query execution failed", idAttr, slog.String("sql", sql))
		return err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "addition succeed", idAttr)
	return nil
}

func (dao *daoPgx) ModifyRec(uow db.UoW, rec DefRec) error {
	idAttr := slog.Any("typeID", rec.TypeRef.TypeID)
	dao.log.Log(uow.Ctx, lf.LevelTrace, "modification started", idAttr)
	dto, err := DataFromDefRec(rec)
	if err != nil {
		dao.log.Error("model conversion failed", idAttr)
		return err
	}
	args := pgx.NamedArgs{
		"desc_id": dto.TypeID,
		"exp_vk":  dto.ExpVK,
	}
	ct, err := uow.Pgx.Exec(uow.Ctx, updateRec, args)
	if err != nil {
		dao.log.Error("query execution failed", idAttr)
		return err
	}
	if ct.RowsAffected() == 0 {
		dao.log.Error("entity update failed", idAttr)
		return errOptimisticUpdate(rec.TypeRef.TypeRN - 1)
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "modification succeed", idAttr)
	return nil
}

func (dao *daoPgx) GetRefs(uow db.UoW) ([]typesem.SemRef, error) {
	rows, err := uow.Pgx.Query(uow.Ctx, selectRefs)
	if err != nil {
		dao.log.Error("query execution failed")
		return nil, err
	}
	defer rows.Close()
	dtos, err := pgx.CollectRows(rows, pgx.RowToStructByName[typesem.SemRefDS])
	if err != nil {
		dao.log.Error("rows scanning failed")
		return nil, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entities selection succeed", slog.Any("dtos", dtos))
	return typesem.DataToRefs(dtos)
}

func (dao *daoPgx) GetRecByRef(uow db.UoW, ref typesem.SemRef) (DefRec, error) {
	refAttr := slog.Any("ref", ref)
	rows, err := uow.Pgx.Query(uow.Ctx, selectRecByID, ref.TypeID.String())
	if err != nil {
		dao.log.Error("query execution failed", refAttr)
		return DefRec{}, err
	}
	defer rows.Close()
	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[defRecDS])
	if err != nil {
		dao.log.Error("row scanning failed", refAttr)
		return DefRec{}, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entity selection succeed", refAttr)
	return DataToDefRec(dto)
}

func (dao *daoPgx) GetRecByQN(uow db.UoW, typeQN uniqsym.ADT) (DefRec, error) {
	qnAttr := slog.Any("typeQN", typeQN)
	rows, err := uow.Pgx.Query(uow.Ctx, selectRecByQN, uniqsym.ConvertToString(typeQN))
	if err != nil {
		dao.log.Error("query execution failed", qnAttr)
		return DefRec{}, err
	}
	defer rows.Close()
	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[defRecDS])
	if err != nil {
		dao.log.Error("row scanning failed", qnAttr)
		return DefRec{}, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entity selection succeed", qnAttr)
	return DataToDefRec(dto)
}

func (dao *daoPgx) GetRecsByRefs(uow db.UoW, refs []typesem.SemRef) (_ []DefRec, err error) {
	if len(refs) == 0 {
		return []DefRec{}, nil
	}
	batch := pgx.Batch{}
	for _, ref := range refs {
		if ref.TypeID.IsEmpty() {
			return nil, identity.ErrEmpty
		}
		batch.Queue(selectRecByID, ref.TypeID.String())
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	var dtos []defRecDS
	for _, defRef := range refs {
		rows, err := br.Query()
		if err != nil {
			dao.log.Error("query execution failed", slog.Any("defRef", defRef), slog.String("q", selectRecByID))
		}
		dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[defRecDS])
		if err != nil {
			dao.log.Error("row scanning failed", slog.Any("defRef", defRef))
		}
		dtos = append(dtos, dto)
	}
	if err != nil {
		return nil, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entities selection succeed", slog.Any("dtos", dtos))
	return DataToDefRecs(dtos)
}

func (dao *daoPgx) SelectEnv(uow db.UoW, typeQNs []uniqsym.ADT) (map[uniqsym.ADT]DefRec, error) {
	recs, err := dao.GetRecsByQNs(uow, typeQNs)
	if err != nil {
		return nil, err
	}
	env := make(map[uniqsym.ADT]DefRec, len(recs))
	for i, root := range recs {
		env[typeQNs[i]] = root
	}
	return env, nil
}

func (dao *daoPgx) GetRecsByQNs(uow db.UoW, typeQNs []uniqsym.ADT) (_ []DefRec, err error) {
	if len(typeQNs) == 0 {
		return []DefRec{}, nil
	}
	batch := pgx.Batch{}
	for _, typeQN := range typeQNs {
		batch.Queue(selectRecByQN, uniqsym.ConvertToString(typeQN))
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	var dtos []defRecDS
	for _, typeQN := range typeQNs {
		rows, err := br.Query()
		if err != nil {
			dao.log.Error("query execution failed", slog.Any("typeQN", typeQN), slog.String("q", selectRecByQN))
		}
		dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[defRecDS])
		if err != nil {
			dao.log.Error("row scanning failed", slog.Any("typeQN", typeQN))
		}
		dtos = append(dtos, dto)
	}
	if err != nil {
		return nil, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entities selection succeed", slog.Any("dtos", dtos))
	return DataToDefRecs(dtos)
}

const (
	updateRec = `
		update proc_type_defs
		set def_rn = @def_rn,
			exp_vk = @exp_vk
		where desc_id = @desc_id
			and def_rn = @def_rn - 1`

	selectRecByQN = `
		select
			td.desc_id,
			td.exp_vk,
			de.desc_rn
		from proc_type_defs td
		left join desc_sems de
			on de.desc_id = td.desc_id
		left join desc_binds db
			on db.desc_id = td.desc_id
		where db.desc_qn = $1`

	selectRecByID = `
		select
			td.desc_id,
			td.exp_vk,
			de.desc_rn
		from proc_type_defs td
		left join desc_sems de
			on de.desc_id = td.desc_id
		where td.desc_id = $1`

	selectRefs = `
		select
			desc_id,
			def_rn
		from proc_type_defs`
)

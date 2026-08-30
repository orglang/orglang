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
	refAttr := slog.Any("ref", rec.TypeRef)
	dao.log.Log(uow.Ctx, lf.LevelTrace, "addition started", refAttr)
	dto, convErr := DataFromDefRec(rec)
	if convErr != nil {
		dao.log.Error("model conversion failed", refAttr)
		return convErr
	}
	sql, args := dao.qb.insertRec(dto)
	_, execErr := uow.Pgx.Exec(uow.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", refAttr, slog.String("sql", sql))
		return execErr
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "addition succeed", refAttr)
	return nil
}

func (dao *daoPgx) Update(uow db.UoW, rec DefRec) error {
	refAttr := slog.Any("ref", rec.TypeRef)
	dao.log.Log(uow.Ctx, lf.LevelTrace, "update started", refAttr)
	dto, convErr := DataFromDefRec(rec)
	if convErr != nil {
		dao.log.Error("model conversion failed", refAttr)
		return convErr
	}
	args := pgx.NamedArgs{
		"desc_id": dto.TypeID,
		"exp_vk":  dto.ExpVK,
	}
	_, execErr := uow.Pgx.Exec(uow.Ctx, updateRec, args)
	if execErr != nil {
		dao.log.Error("query execution failed", refAttr)
		return execErr
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "update succeed", refAttr)
	return nil
}

func (dao *daoPgx) SelectRefs(uow db.UoW) ([]typesem.SemRef, error) {
	rows, err := uow.Pgx.Query(uow.Ctx, selectRefs)
	if err != nil {
		dao.log.Error("query execution failed", slog.String("q", selectRefs))
		return nil, err
	}
	defer rows.Close()
	dtos, err := pgx.CollectRows(rows, pgx.RowToStructByName[typesem.SemRefDS])
	if err != nil {
		dao.log.Error("rows scanning failed")
		return nil, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "getting succeed", slog.Any("dtos", dtos))
	return typesem.DataToRefs(dtos)
}

func (dao *daoPgx) SelectRecByRef(uow db.UoW, ref typesem.SemRef) (DefRec, error) {
	refAttr := slog.Any("defRef", ref)
	rows, err := uow.Pgx.Query(uow.Ctx, selectRecByID, ref.TypeID.String())
	if err != nil {
		dao.log.Error("query execution failed", refAttr, slog.String("q", selectRecByID))
		return DefRec{}, err
	}
	defer rows.Close()
	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[defRecDS])
	if err != nil {
		dao.log.Error("row scanning failed", refAttr)
		return DefRec{}, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "getting succeed", refAttr)
	return DataToDefRec(dto)
}

func (dao *daoPgx) SelectRecByQN(uow db.UoW, xactQN uniqsym.ADT) (DefRec, error) {
	qnAttr := slog.Any("xactQN", xactQN)
	rows, err := uow.Pgx.Query(uow.Ctx, selectRecByQN, uniqsym.ConvertToString(xactQN))
	if err != nil {
		dao.log.Error("query execution failed", qnAttr, slog.String("q", selectRecByQN))
		return DefRec{}, err
	}
	defer rows.Close()
	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[defRecDS])
	if err != nil {
		dao.log.Error("row scanning failed", qnAttr)
		return DefRec{}, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "getting succeed", qnAttr)
	return DataToDefRec(dto)
}

func (dao *daoPgx) SelectRecsByRefs(uow db.UoW, refs []typesem.SemRef) (_ []DefRec, err error) {
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
	dao.log.Log(uow.Ctx, lf.LevelTrace, "getting succeed", slog.Any("dtos", dtos))
	return DataToDefRecs(dtos)
}

func (dao *daoPgx) GetRecsByQNs(uow db.UoW, typeQNs []uniqsym.ADT) (_ map[uniqsym.ADT]DefRec, err error) {
	if len(typeQNs) == 0 {
		return map[uniqsym.ADT]DefRec{}, nil
	}
	batch := pgx.Batch{}
	sql := dao.qb.selectRecByQN()
	for _, typeQN := range typeQNs {
		batch.Queue(sql, uniqsym.ConvertToString(typeQN))
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	dtos := make(map[uniqsym.ADT]defRecDS, len(typeQNs))
	for _, typeQN := range typeQNs {
		qnAttr := slog.Any("qn", typeQN)
		rows, readErr := br.Query()
		if readErr != nil {
			dao.log.Error("query execution failed", qnAttr, slog.Any("sql", sql))
			return map[uniqsym.ADT]DefRec{}, readErr
		}
		dto, scanErr := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[defRecDS])
		if scanErr != nil {
			dao.log.Error("row scanning failed", qnAttr)
			return map[uniqsym.ADT]DefRec{}, scanErr
		}
		dtos[typeQN] = dto
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "getting succeed", slog.Any("dtos", dtos))
	return DataToDefRecMap(dtos)
}

const (
	updateRec = `
		update pool_type_defs
		set def_rn = @def_rn,
			exp_vk = @exp_vk
		where desc_id = @desc_id
			and def_rn = @def_rn - 1`

	selectRefs = `
		select
			desc_id,
			def_rn
		from pool_type_defs`

	selectRecByQN = `
		select
			xd.desc_id,
			xd.def_rn,
			xd.exp_vk
		from pool_type_defs xd
		left join desc_binds db
			on db.desc_id = xd.desc_id
		where db.desc_qn = $1`

	selectRecByID = `
		select
			desc_id,
			def_rn,
			exp_vk
		from pool_type_defs
		where desc_id = $1`
)

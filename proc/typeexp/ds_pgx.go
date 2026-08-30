package typeexp

import (
	"errors"
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/valkey"
)

// Adapter
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

func (dao *daoPgx) AddRec(uow db.UoW, rec ExpRec) (err error) {
	vkAttr := slog.Any("expVK", rec.Key())
	dto := dataFromExpRec(rec)
	batch := pgx.Batch{}
	for _, st := range dto.States {
		sql, args := dao.qb.insertRec(st)
		batch.Queue(sql, args...)
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	for range dto.States {
		_, readErr := br.Exec()
		if readErr != nil {
			dao.log.Error("query execution failed", vkAttr)
			return readErr
		}
	}
	return nil
}

func (dao *daoPgx) SelectRecByVK(uow db.UoW, expVK valkey.ADT) (ExpRec, error) {
	vkAttr := slog.Any("expVK", expVK)
	rows, queryErr := uow.Pgx.Query(uow.Ctx, selectByID, valkey.ConvertToInt(expVK))
	if queryErr != nil {
		dao.log.Error("query execution failed", vkAttr)
		return nil, queryErr
	}
	defer rows.Close()
	dtos, scanErr := pgx.CollectRows(rows, pgx.RowToStructByName[stateDS])
	if scanErr != nil {
		dao.log.Error("row scanning failed", vkAttr)
		return nil, scanErr
	}
	if len(dtos) == 0 { // revive:disable-line
		dao.log.Error("entity selection failed", vkAttr)
		return nil, errors.New("no rows selected")
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entity selection succeed", slog.Any("dtos", dtos))
	states := make(map[int64]stateDS, len(dtos))
	for _, dto := range dtos {
		states[dto.ExpVK] = dto
	}
	return statesToExpRec(states, states[valkey.ConvertToInt(expVK)])
}

func (dao *daoPgx) SelectEnv(uow db.UoW, expVKs []valkey.ADT) (map[valkey.ADT]ExpRec, error) {
	recs, err := dao.SelectRecsByVKs(uow, expVKs)
	if err != nil {
		return nil, err
	}
	env := make(map[valkey.ADT]ExpRec, len(recs))
	for _, rec := range recs {
		env[rec.Key()] = rec
	}
	return env, nil
}

func (dao *daoPgx) SelectRecsByVKs(uow db.UoW, expVKs []valkey.ADT) (_ []ExpRec, err error) {
	batch := pgx.Batch{}
	for _, expVK := range expVKs {
		batch.Queue(selectByID, valkey.ConvertToInt(expVK))
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	var recs []ExpRec
	for _, expVK := range expVKs {
		vkAttr := slog.Any("expVK", expVK)
		rows, err := br.Query()
		if err != nil {
			dao.log.Error("query execution failed", vkAttr)
		}
		dtos, err := pgx.CollectRows(rows, pgx.RowToStructByName[stateDS])
		if err != nil {
			dao.log.Error("rows scanning failed", vkAttr)
		}
		if len(dtos) == 0 {
			dao.log.Error("entity selection failed", vkAttr)
			return nil, ErrDoesNotExist(expVK)
		}
		rec, err := dataToExpRec(expRecDS{valkey.ConvertToInt(expVK), dtos})
		if err != nil {
			dao.log.Error("model conversion failed", vkAttr)
			return nil, err
		}
		recs = append(recs, rec)
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entities selection succeed", slog.Any("recs", recs))
	return recs, err
}

const (
	insertRec = `
		insert into proc_type_exps (
			exp_vk, sup_exp_vk, desc_id, desc_rn, kind, spec
		) values (
			@exp_vk, @sup_exp_vk, @desc_id, @desc_rn, @kind, @spec
		)
		on conflict (exp_vk) do nothing`

	selectByID = `
		with recursive exp_tree AS (
			select top.*
			from proc_type_exps top
			where exp_vk = $1
			union all
			select sub.*
			from proc_type_exps sub, exp_tree sup
			where sub.sup_exp_vk = sup.exp_vk
		)
		select * from exp_tree`
)

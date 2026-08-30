package compexec

import (
	"errors"
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/compsem"
	"orglang/go-engine/adt/compvar"
	"orglang/go-engine/adt/seqnum"
)

// Adapter
type daoPgx struct {
	qb  queryBuilder
	log *slog.Logger
}

// for compilation purposes
func newRepo() Repo {
	return new(daoPgx)
}

func newDaoPgx(qb queryBuilder, log *slog.Logger) *daoPgx {
	name := slog.String("name", reflect.TypeFor[daoPgx]().Name())
	return &daoPgx{qb, log.With(name)}
}

func (dao *daoPgx) AddRec(uow db.UoW, rec ExecRec) error {
	dto := DataFromExecRec(rec)
	refAttr := slog.Any("ref", rec.CompRef)
	sql, args := dao.qb.insertRec(dto)
	_, execErr := uow.Pgx.Exec(uow.Ctx, sql, args...)
	if execErr != nil {
		dao.log.Error("query execution failed", refAttr, slog.String("sql", sql))
		return execErr
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "insertion succeed", slog.Any("dto", dto))
	return nil
}

func (dao *daoPgx) GetSnapByRef(uow db.UoW, ref compsem.SemRef) (ExecSnap, error) {
	refAttr := slog.Any("ref", ref)
	chnlRows, err := uow.Pgx.Query(uow.Ctx, selectChnls, ref.CompID.String())
	if err != nil {
		dao.log.Error("query execution failed", refAttr)
		return ExecSnap{}, err
	}
	defer chnlRows.Close()
	chnlDtos, err := pgx.CollectRows(chnlRows, pgx.RowToStructByName[compvar.VarRecDS])
	if err != nil {
		dao.log.Error("row scanning failed", refAttr, slog.Any("t", reflect.TypeOf(chnlDtos)))
		return ExecSnap{}, err
	}
	chnls, err := compvar.DataToLinearRecs(chnlDtos)
	if err != nil {
		dao.log.Error("model conversion failed", refAttr)
		return ExecSnap{}, err
	}
	dao.log.Debug("snap selection succeed", refAttr)
	return ExecSnap{
		LinearVars: compvar.IndexBy(ChnlPH, chnls),
	}, nil
}

func (dao *daoPgx) ModifyRec(uow db.UoW, mod ExecMod) (err error) {
	if len(mod.CompRefs) == 0 {
		panic("empty locks")
	}
	dto, err := DataFromMod(mod)
	if err != nil {
		dao.log.Error("conversion failed")
		return err
	}
	// binds
	bindReq := pgx.Batch{}
	for _, dto := range dto.LinearVars {
		args := pgx.NamedArgs{
			"impl_id":  dto.CompID,
			"chnl_ph":  dto.ChnlPH,
			"chnl_id":  dto.ChnlID,
			"state_id": dto.ExpVK,
		}
		bindReq.Queue(insertBind, args)
	}
	if bindReq.Len() > 0 {
		bindRes := uow.Pgx.SendBatch(uow.Ctx, &bindReq)
		defer func() {
			err = errors.Join(err, bindRes.Close())
		}()
		for _, dto := range dto.LinearVars {
			_, err = bindRes.Exec()
			if err != nil {
				dao.log.Error("execution failed", slog.Any("dto", dto))
			}
		}
		if err != nil {
			return err
		}
	}
	// execs
	execReq := pgx.Batch{}
	for _, dto := range dto.CompRefs {
		args := pgx.NamedArgs{
			"impl_id": dto.CompID,
			"impl_rn": dto.CompRN,
		}
		execReq.Queue(updateExec, args)
	}
	execRes := uow.Pgx.SendBatch(uow.Ctx, &execReq)
	defer func() {
		err = errors.Join(err, execRes.Close())
	}()
	for _, dto := range dto.CompRefs {
		ct, err := execRes.Exec()
		if err != nil {
			dao.log.Error("execution failed", slog.Any("dto", dto))
		}
		if ct.RowsAffected() == 0 {
			dao.log.Error("update failed")
			return errOptimisticUpdate(seqnum.ADT(dto.CompRN))
		}
	}
	if err != nil {
		return err
	}
	dao.log.Debug("update succeed")
	return nil
}

const (
	insertBind = `
		insert into proc_binds (
			impl_id, chnl_ph, chnl_id, state_id, impl_rn
		) values (
			@impl_id, @chnl_ph, @chnl_id, @state_id, @impl_rn
		)`

	updateExec = `
		update proc_comp_execs
		set impl_rn = @impl_rn + 1
		where impl_id = @impl_id
			and impl_rn = @impl_rn`

	selectChnls = `
		with bnds as not materialized (
			select distinct on (chnl_ph)
				*
			from proc_bnds
			where proc_id = 'proc1'
			order by chnl_ph, abs(rev) desc
		), liabs as not materialized (
			select distinct on (proc_id)
				*
			from pool_liabs
			where proc_id = 'proc1'
			order by proc_id, abs(rev) desc
		)
		select
			bnd.*,
			prvd.desc_id
		from bnds bnd
		left join liabs liab
			on liab.proc_id = bnd.proc_id
		left join pool_comp_execs prvd
			on prvd.desc_id = liab.desc_id`
)

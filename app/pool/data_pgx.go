package pool

import (
	"database/sql"
	"errors"
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"

	"smecalculus/rolevod/lib/core"
	"smecalculus/rolevod/lib/data"
	"smecalculus/rolevod/lib/id"
	"smecalculus/rolevod/lib/rev"

	"smecalculus/rolevod/internal/chnl"
	"smecalculus/rolevod/internal/proc"
	"smecalculus/rolevod/internal/step"
)

// Adapter
type repoPgx struct {
	log *slog.Logger
}

func newRepoPgx(l *slog.Logger) *repoPgx {
	name := slog.String("name", "poolRepoPgx")
	return &repoPgx{l.With(name)}
}

// for compilation purposes
func newRepo() Repo {
	return &repoPgx{}
}

func (r *repoPgx) Insert(source data.Source, root Root) (err error) {
	ds := data.MustConform[data.SourcePgx](source)
	dto := DataFromRoot(root)
	args := pgx.NamedArgs{
		"pool_id":     dto.PoolID,
		"title":       dto.Title,
		"sup_pool_id": dto.SupID,
		"revs":        dto.Revs,
	}
	_, err = ds.Conn.Exec(ds.Ctx, insertRoot, args)
	if err != nil {
		r.log.Error("execution failed", slog.String("q", insertRoot))
		return err
	}
	r.log.Debug("insertion succeeded", slog.Any("poolID", root.PoolID))
	return nil
}

func (r *repoPgx) SelectAssets(source data.Source, poolID id.ADT) (AssetSnap, error) {
	ds := data.MustConform[data.SourcePgx](source)
	idAttr := slog.Any("poolID", poolID)
	rows, err := ds.Conn.Query(ds.Ctx, selectAssetSnap, poolID.String())
	if err != nil {
		r.log.Error("execution failed", idAttr, slog.String("q", selectAssetSnap))
		return AssetSnap{}, err
	}
	defer rows.Close()
	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[assetSnapData])
	if err != nil {
		r.log.Error("collection failed", idAttr, slog.Any("t", reflect.TypeOf(dto)))
		return AssetSnap{}, err
	}
	r.log.Debug("selection succeeded", idAttr)
	return DataToAssetSnap(dto)
}

func (r *repoPgx) SelectProc(source data.Source, procID id.ADT) (proc.Snap, error) {
	ds := data.MustConform[data.SourcePgx](source)
	idAttr := slog.Any("procID", procID)
	chnlRows, err := ds.Conn.Query(ds.Ctx, selectChnls, procID.String())
	if err != nil {
		r.log.Error("execution failed", idAttr, slog.String("q", selectChnls))
		return proc.Snap{}, err
	}
	defer chnlRows.Close()
	chnlDtos, err := pgx.CollectRows(chnlRows, pgx.RowToStructByName[epData])
	if err != nil {
		r.log.Error("collection failed", idAttr, slog.Any("t", reflect.TypeOf(chnlDtos)))
		return proc.Snap{}, err
	}
	chnls, err := DataToEPs(chnlDtos)
	if err != nil {
		r.log.Error("mapping failed", idAttr)
		return proc.Snap{}, err
	}
	stepRows, err := ds.Conn.Query(ds.Ctx, selectSteps, procID.String())
	if err != nil {
		r.log.Error("execution failed", idAttr, slog.String("q", selectSteps))
		return proc.Snap{}, err
	}
	defer stepRows.Close()
	stepDtos, err := pgx.CollectRows(stepRows, pgx.RowToStructByName[step.RootData])
	if err != nil {
		r.log.Error("collection failed", idAttr, slog.Any("t", reflect.TypeOf(stepDtos)))
		return proc.Snap{}, err
	}
	steps, err := step.DataToRoots(stepDtos)
	if err != nil {
		r.log.Error("mapping failed", idAttr)
		return proc.Snap{}, err
	}
	r.log.Debug("selection succeeded", idAttr)
	return proc.Snap{
		Chnls: core.IndexBy(proc.ChnlPH, chnls),
		Steps: core.IndexBy(step.ChnlID, steps),
	}, nil
}

func (r *repoPgx) UpdateProc(source data.Source, mod proc.Mod) (err error) {
	ds := data.MustConform[data.SourcePgx](source)
	dto := proc.DataFromMod(mod)
	// bindings
	bndReq := pgx.Batch{}
	for _, dto := range dto.Bnds {
		args := pgx.NamedArgs{
			"proc_id":  dto.ProcID,
			"chnl_ph":  dto.ChnlPH,
			"chnl_id":  dto.ChnlID,
			"state_id": dto.StateID,
			"rev":      dto.Rev,
		}
		bndReq.Queue(insertBnd, args)
	}
	bndRes := ds.Conn.SendBatch(ds.Ctx, &bndReq)
	defer func() {
		err = errors.Join(err, bndRes.Close())
	}()
	for _, dto := range dto.Bnds {
		_, err := bndRes.Exec()
		if err != nil {
			r.log.Error("execution failed", slog.String("q", insertBnd), slog.Any("sto", dto))
		}
	}
	if err != nil {
		return err
	}
	// steps
	stepReq := pgx.Batch{}
	for _, dto := range dto.Steps {
		args := pgx.NamedArgs{
			"proc_id": dto.PID,
			"chnl_id": dto.VID,
			"kind":    dto.K,
			"spec":    dto.Spec,
		}
		stepReq.Queue(insertStep, args)
	}
	stepRes := ds.Conn.SendBatch(ds.Ctx, &stepReq)
	defer func() {
		err = errors.Join(err, bndRes.Close())
	}()
	for _, dto := range dto.Steps {
		_, err := stepRes.Exec()
		if err != nil {
			r.log.Error("execution failed", slog.String("q", insertStep), slog.Any("dto", dto))
		}
	}
	if err != nil {
		return err
	}
	// roots
	rootReq := pgx.Batch{}
	for _, dto := range dto.Locks {
		args := pgx.NamedArgs{
			"pool_id": dto.PoolID,
			"rev":     dto.Rev,
			"k":       procRev,
		}
		rootReq.Queue(updateRoot, args)
	}
	rootRes := ds.Conn.SendBatch(ds.Ctx, &rootReq)
	defer func() {
		err = errors.Join(err, rootRes.Close())
	}()
	for _, dto := range dto.Locks {
		ct, err := rootRes.Exec()
		if err != nil {
			r.log.Error("execution failed", slog.String("q", updateRoot), slog.Any("dto", dto))
		}
		if ct.RowsAffected() == 0 {
			r.log.Error("update failed")
			return errOptimisticUpdate(rev.ADT(dto.Rev))
		}
	}
	if err != nil {
		return err
	}
	r.log.Debug("update succeeded")
	return nil
}

func (r *repoPgx) UpdateAssets(source data.Source, mod AssetMod) (err error) {
	ds := data.MustConform[data.SourcePgx](source)
	idAttr := slog.Any("id", mod.OutPoolID)
	dto := DataFromAssetMod(mod)
	batch := pgx.Batch{}
	for _, ep := range dto.EPs {
		args := pgx.NamedArgs{
			"pool_id": dto.InPoolID,
			// proc_id ???
			"chnl_id":    ep.ChnlID,
			"state_id":   ep.StateID,
			"ex_pool_id": dto.OutPoolID,
			"rev":        dto.Rev,
		}
		batch.Queue(insertAsset, args)
	}
	br := ds.Conn.SendBatch(ds.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	for _, ep := range dto.EPs {
		_, err := br.Exec()
		if err != nil {
			r.log.Error("execution failed", idAttr, slog.String("q", insertAsset), slog.Any("ep", ep))
		}
	}
	if err != nil {
		return err
	}
	args := pgx.NamedArgs{
		"pool_id": dto.OutPoolID,
		"rev":     dto.Rev,
	}
	ct, err := ds.Conn.Exec(ds.Ctx, updateRoot, args)
	if err != nil {
		r.log.Error("execution failed", idAttr, slog.String("q", updateRoot))
		return err
	}
	if ct.RowsAffected() == 0 {
		r.log.Error("update failed", idAttr)
		return errOptimisticUpdate(mod.Rev)
	}
	r.log.Log(ds.Ctx, core.LevelTrace, "update succeeded", idAttr)
	return nil
}

func (r *repoPgx) SelectSubs(source data.Source, poolID id.ADT) (SubSnap, error) {
	ds := data.MustConform[data.SourcePgx](source)
	idAttr := slog.Any("poolID", poolID)
	rows, err := ds.Conn.Query(ds.Ctx, selectOrgSnap, poolID.String())
	if err != nil {
		r.log.Error("execution failed", idAttr, slog.String("q", selectOrgSnap))
		return SubSnap{}, err
	}
	defer rows.Close()
	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[subSnapData])
	if err != nil {
		r.log.Error("collection failed", idAttr, slog.Any("t", reflect.TypeOf(dto)))
		return SubSnap{}, err
	}
	r.log.Debug("selection succeeded", idAttr)
	return DataToSubSnap(dto)
}

func (r *repoPgx) SelectRefs(source data.Source) ([]Ref, error) {
	ds := data.MustConform[data.SourcePgx](source)
	query := `
		select
			pool_id, title
		from pool_roots`
	rows, err := ds.Conn.Query(ds.Ctx, query)
	if err != nil {
		r.log.Error("execution failed", slog.String("q", query))
		return nil, err
	}
	defer rows.Close()
	dtos, err := pgx.CollectRows(rows, pgx.RowToStructByName[refData])
	if err != nil {
		r.log.Error("collection failed", slog.Any("t", reflect.TypeOf(dtos)))
		return nil, err
	}
	return DataToRefs(dtos)
}

func (r *repoPgx) Transfer(source data.Source, giver id.ADT, taker id.ADT, pids []chnl.ID) (err error) {
	ds := data.MustConform[data.SourcePgx](source)
	query := `
		insert into consumers (
			giver_id, taker_id, chnl_id
		) values (
			@giver_id, @taker_id, @chnl_id
		)`
	batch := pgx.Batch{}
	for _, pid := range pids {
		args := pgx.NamedArgs{
			"giver_id": sql.NullString{String: giver.String(), Valid: !giver.IsEmpty()},
			"taker_id": taker.String(),
			"chnl_id":  pid.String(),
		}
		batch.Queue(query, args)
	}
	br := ds.Conn.SendBatch(ds.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	for _, pid := range pids {
		_, err := br.Exec()
		if err != nil {
			r.log.Error("query execution failed",
				slog.Any("reason", err),
				slog.Any("id", pid),
			)
		}
	}
	if err != nil {
		return err
	}
	r.log.Log(ds.Ctx, core.LevelTrace, "context transfer succeeded")
	return nil
}

const (
	insertRoot = `
		insert into pool_roots (
			pool_id, revs, title, sup_pool_id
		) values (
			@pool_id, @revs, @title, @sup_pool_id
		)`

	insertAsset = `
		insert into pool_assets (
			pool_id, chnl_key, state_id, ex_pool_id, rev
		) values (
			@pool_id, @chnl_key, @state_id, @ex_pool_id, @rev
		)`

	insertBnd = `
		insert into pool_assets (
			pool_id, chnl_key, state_id, ex_pool_id, rev
		) values (
			@pool_id, @chnl_key, @state_id, @ex_pool_id, @rev
		)`

	insertStep = `
		insert into pool_steps (
			proc_id, chnl_id, kind, spec
		) values (
			@proc_id, @chnl_id, @kind, @spec
		)`

	updateRoot = `
		update pool_roots
		set revs[@k] = @rev + 1
		where pool_id = @pool_id
			and revs[@k] = @rev`

	selectOrgSnap = `
		select
			sup.pool_id,
			sup.title,
			jsonb_agg(json_build_object('pool_id', sub.pool_id, 'title', sub.title)) as subs
		from pool_roots sup
		left join pool_sups rel
			on rel.sup_pool_id = sup.pool_id
		left join pool_roots sub
			on sub.pool_id = rel.pool_id
			and sub.revs[1] = rel.rev
		where sup.pool_id = $1
		group by sup.pool_id, sup.title`

	selectAssetSnap = `
		select
			r.pool_id,
			r.title,
			jsonb_agg(json_build_object('chnl_key', a.chnl_key, 'state_id', a.state_id)) as ctx
		from pool_roots r
		left join pool_assets a
			on a.pool_id = r.pool_id
			and a.rev = r.revs[2]
		where r.pool_id = $1
		group by r.pool_id, r.title`

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
			prvd.pool_id
		from bnds bnd
		left join liabs liab
			on liab.proc_id = bnd.proc_id
		left join pool_roots prvd
			on prvd.pool_id = liab.pool_id`

	selectSteps = ``
)

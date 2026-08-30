package commturn

import (
	"errors"
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"

	"orglang/go-engine/lib/db"
	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/identity"
)

type daoPgx struct {
	log *slog.Logger
}

// for compilation purposes
func newRepo() Repo {
	return new(daoPgx)
}

func newDaoPgx(l *slog.Logger) *daoPgx {
	name := slog.String("name", reflect.TypeFor[daoPgx]().Name())
	return &daoPgx{l.With(name)}
}

func (dao *daoPgx) InsertRecs(uow db.UoW, recs ...TurnRec) error {
	dtos, err := DataFromStepRecs(recs)
	if err != nil {
		dao.log.Error("conversion failed")
		return err
	}
	batch := pgx.Batch{}
	for _, dto := range dtos {
		args := pgx.NamedArgs{
			"kind":    dto.K,
			"comp_id": dto.ExecID,
			"chnl_id": dto.ChnlID,
			"proc_er": dto.ProcER,
		}
		batch.Queue(insertStep, args)
	}
	br := uow.Pgx.SendBatch(uow.Ctx, &batch)
	defer func() {
		err = errors.Join(err, br.Close())
	}()
	for _, dto := range dtos {
		_, err = br.Exec()
		if err != nil {
			dao.log.Error("execution failed", slog.String("q", insertStep), slog.Any("dto", dto))
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (dao *daoPgx) SelectRecs(uow db.UoW, rid identity.ADT) (TurnRec, error) {
	query := `
		select
			id, kind, pid, vid, spec
		from steps
		WHERE id = $1`
	return dao.execute(uow, query, rid.String())
}

func (dao *daoPgx) execute(uow db.UoW, query string, arg string) (TurnRec, error) {
	rows, err := uow.Pgx.Query(uow.Ctx, query, arg)
	if err != nil {
		dao.log.Error("query execution failed", slog.String("q", query))
		return nil, err
	}
	defer rows.Close()
	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[StepRecDS])
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		dao.log.Error("row scanning failed")
		return nil, err
	}
	root, err := dataToStepRec(dto)
	if err != nil {
		dao.log.Error("model conversion failed")
		return nil, err
	}
	dao.log.Log(uow.Ctx, lf.LevelTrace, "entity selection succeed", slog.Any("root", root))
	return root, nil
}

const (
	insertStep = `
		insert into pool_comm_turns (
			id, kind, pid, vid, spec
		) values (
			@id, @kind, @pid, @vid, @spec
		)`
)

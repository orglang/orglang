package commturn

import (
	"database/sql"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/proc/termexp"
)

type Repo interface {
	InsertRecs(db.UoW, ...TurnRec) error
}

type StepRecDS struct {
	K      stepKindDS       `db:"kind"`
	ExecID sql.NullString   `db:"impl_id"`
	ChnlID sql.NullString   `db:"chnl_id"`
	ProcER termexp.ExpRecDS `db:"proc_er"`
}

type stepKindDS int

const (
	nonStep = stepKindDS(iota)
	msgStep
	svcStep
)

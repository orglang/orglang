package commexch

import (
	"database/sql"
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/commsem"
	"orglang/go-engine/adt/uniqsym"
	"orglang/go-engine/pool/commturn"
)

type Repo interface {
	AddRec(db.UoW, ExchRec) error
	ModifyRec(db.UoW, ExchMod) error
	GetRefsByQNs(db.UoW, []uniqsym.ADT) (map[uniqsym.ADT]commsem.SemRef, error)
	GetSnapByQry(db.UoW, ExchQry) (ExchSnap, error)
}

type exchRecDS struct {
	CommID   string `db:"comm_id"`
	CommRN   int64  `db:"comm_rn"`
	OffsetNr int64  `db:"offset_nr"`
}

type exchModDS struct {
	CommID   string          `db:"comm_id"`
	OffsetNr sql.Null[int64] `db:"offset_nr"`
}

type exchQryDS struct {
	CommID string           `db:"comm_id"`
	ChnlID sql.Null[string] `db:"chnl_id"`
}

type exchSnapDS struct {
	CommID   string               `db:"comm_id"`
	CommRN   int64                `db:"comm_rn"`
	OffsetNr int64                `db:"offset_nr"`
	Turns    []commturn.TurnRecDS `db:"turns"`
}

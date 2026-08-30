package commturn

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/pool/termexp"
)

type Repo interface {
	AddRec(db.UoW, TurnRec) error
	AddRecs(db.UoW, []TurnRec) error
}

type TurnRecDS struct {
	CommID string           `db:"comm_id"`
	CommRN int64            `db:"comm_rn"`
	CompID string           `db:"comp_id"`
	ChnlID string           `db:"chnl_id"`
	K      turnKind         `db:"kind"`
	Exp    termexp.ExpRecDS `db:"exp" fieldopt:"noexpand"`
}

type turnKind int16

const (
	unkKind turnKind = iota
	PubKind
	SubKind
)

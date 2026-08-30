package typesem

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/uniqsym"
)

type Repo interface {
	TouchRef(db.UoW, SemRef) error
	GetRefsByQNs(db.UoW, []uniqsym.ADT) (map[uniqsym.ADT]SemRef, error)
}

type SemRefDS struct {
	TypeID string `db:"type_id"`
	TypeRN int64  `db:"type_rn"`
}

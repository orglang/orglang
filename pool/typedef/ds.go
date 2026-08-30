package typedef

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/adt/uniqsym"
)

type Repo interface {
	AddRec(db.UoW, DefRec) error
	Update(db.UoW, DefRec) error
	SelectRefs(db.UoW) ([]typesem.SemRef, error)
	SelectRecByRef(db.UoW, typesem.SemRef) (DefRec, error)
	SelectRecsByRefs(db.UoW, []typesem.SemRef) ([]DefRec, error)
	SelectRecByQN(db.UoW, uniqsym.ADT) (DefRec, error)
	GetRecsByQNs(db.UoW, []uniqsym.ADT) (map[uniqsym.ADT]DefRec, error)
}

type defRecDS struct {
	TypeID string `db:"type_id"`
	TypeRN int64  `db:"type_rn"`
	ExpVK  int64  `db:"exp_vk"`
}

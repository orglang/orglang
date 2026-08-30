package typedef

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/adt/uniqsym"
)

type Repo interface {
	AddRec(db.UoW, DefRec) error
	ModifyRec(db.UoW, DefRec) error
	GetRefs(db.UoW) ([]typesem.SemRef, error)
	GetRecByRef(db.UoW, typesem.SemRef) (DefRec, error)
	GetRecsByRefs(db.UoW, []typesem.SemRef) ([]DefRec, error)
	GetRecByQN(db.UoW, uniqsym.ADT) (DefRec, error)
	GetRecsByQNs(db.UoW, []uniqsym.ADT) ([]DefRec, error)
	SelectEnv(db.UoW, []uniqsym.ADT) (map[uniqsym.ADT]DefRec, error)
}

type defRecDS struct {
	TypeID string `db:"type_id"`
	TypeRN int64  `db:"type_rn"`
	ExpVK  int64  `db:"exp_vk"`
}

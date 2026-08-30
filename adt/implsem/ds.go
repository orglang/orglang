package implsem

import (
	"orglang/go-engine/lib/db"
)

type Repo interface {
	AddRec(db.UoW, SemRec) error
}

type SemRecDS struct {
	ImplQN string `db:"impl_qn"`
	ImplID string `db:"impl_id"`
	Kind   int16  `db:"kind"`
}

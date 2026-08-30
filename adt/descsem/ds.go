package descsem

import (
	"orglang/go-engine/lib/db"
)

type Repo interface {
	AddRec(db.UoW, SemRec) error
}

type SemRecDS struct {
	DescQN string `db:"desc_qn"`
	DescID string `db:"desc_id"`
	Kind   int16  `db:"kind"`
}

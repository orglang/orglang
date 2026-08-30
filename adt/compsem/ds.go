package compsem

import (
	"orglang/go-engine/lib/db"
)

type Repo interface {
	TouchRef(db.UoW, SemRef) error
}

type SemRefDS struct {
	CompID string `db:"comp_id"`
	CompRN int64  `db:"comp_rn"`
}

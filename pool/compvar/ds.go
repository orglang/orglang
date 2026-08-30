package compvar

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/compvar"
)

type Repo interface {
	AddRecs(db.UoW, []compvar.VarRec) error
}

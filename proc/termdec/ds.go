package termdec

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/identity"
	"orglang/go-engine/adt/termsem"
	"orglang/go-engine/adt/termvar"
)

type Repo interface {
	AddRec(db.UoW, DecRec) error
	GetRefs(db.UoW) ([]termsem.SemRef, error)
	GetSnap(db.UoW, termsem.SemRef) (DecSnap, error)
	GetRecs(db.UoW, []identity.ADT) ([]DecRec, error)
	SelectEnv(db.UoW, []identity.ADT) (map[identity.ADT]DecRec, error)
}

type decRecDS struct {
	TermID    string             `db:"term_id"`
	TermRN    int64              `db:"term_rn"`
	LiabVar   termvar.VarRecDS   `db:"liab_var" fieldopt:"noexpand"`
	AssetVars []termvar.VarRecDS `db:"asset_vars"`
}

type decSnapDS struct {
	TermID    string             `db:"term_id"`
	TermRN    int64              `db:"term_rn"`
	LiabVar   termvar.VarRecDS   `db:"liab_var"`
	AssetVars []termvar.VarRecDS `db:"asset_vars"`
}

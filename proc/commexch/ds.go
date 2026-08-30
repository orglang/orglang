package commexch

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/commsem"
	"orglang/go-engine/adt/uniqsym"
)

type Repo interface {
	AddRec(db.UoW, ExchRec) error
	Modifyec(db.UoW, ExchMod) error
	GetRefsByQNs(db.UoW, []uniqsym.ADT) (map[uniqsym.ADT]commsem.SemRef, error)
	GetSnapByQry(db.UoW, ExchQry) (ExchSnap, error)
}

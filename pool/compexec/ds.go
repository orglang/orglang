package compexec

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/compsem"
	"orglang/go-engine/adt/compvar"
	"orglang/go-engine/adt/uniqsym"
)

type Repo interface {
	AddRec(db.UoW, ExecRec) error
	ModifyRec(db.UoW, ExecMod) error
	GetSnapByRef(db.UoW, compsem.SemRef) (ExecSnap2, error)
	GetSnapMapByQNs(db.UoW, []uniqsym.ADT) (map[uniqsym.ADT]ExecSnap1, error)
}

type execRec struct {
	CompID   string `db:"comp_id"`
	CompRN   int64  `db:"comp_rn"`
	LiabMode int16  `db:"liab_mode"`
}

type execSnap1 struct {
	CompRef   compsem.SemRefDS  `db:"sem"`
	LiabMode  int16             `db:"exec.liab_mode"`
	StructVar *compvar.VarRecDS `db:"struct_var"`
	LinearVar *compvar.VarRecDS `db:"linear_var"`
}

type execSnap2 struct {
	CompID     string             `db:"comp_id"`
	CompRN     int64              `db:"comp_rn"`
	LiabMode   int16              `db:"liab_mode"`
	StructVars []compvar.VarRecDS `db:"struct_vars"`
	LinearVars []compvar.VarRecDS `db:"linear_vars"`
}

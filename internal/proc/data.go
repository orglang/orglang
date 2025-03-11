package proc

import (
	"smecalculus/rolevod/internal/step"
)

type modData struct {
	Locks []lockData
	Bnds  []bndData
	Steps []step.RootData
}

type lockData struct {
	PoolID string
	Rev    int
}

type bndData struct {
	ProcID  string
	ChnlPH  string
	ChnlID  string
	StateID string
	Rev     int
}

// goverter:variables
// goverter:output:format assign-variable
// goverter:extend smecalculus/rolevod/lib/id:Convert.*
var (
	DataFromMod func(Mod) modData
	DataFromBnd func(Bnd) bndData
)

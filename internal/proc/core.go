package proc

import (
	"fmt"
	"smecalculus/rolevod/lib/id"
	"smecalculus/rolevod/lib/ph"
	"smecalculus/rolevod/lib/rev"

	"smecalculus/rolevod/internal/chnl"
	"smecalculus/rolevod/internal/step"
)

// aka Configuration
type Snap struct {
	ProcID id.ADT
	Chnls  map[ph.ADT]Chnl
	Steps  map[chnl.ID]step.Root
	PoolID id.ADT
	Rev    rev.ADT
}

type Chnl struct {
	ChnlPH  ph.ADT
	ChnlID  id.ADT
	StateID id.ADT
	// provider
	PoolID id.ADT
}

type Lock struct {
	PoolID id.ADT
	Rev    rev.ADT
}

func ChnlPH(ch Chnl) ph.ADT { return ch.ChnlPH }

func ChnlID(ch Chnl) id.ADT { return ch.ChnlID }

// ответственность за процесс
type Liab struct {
	ProcID id.ADT
	PoolID id.ADT
	// позитивное значение при возникновении
	// негативное значение при лишении
	Rev rev.ADT
}

type Mod struct {
	Locks []Lock
	Bnds  []Bnd
	Steps []step.Root
	Liabs []Liab
}

type Bnd struct {
	ProcID  id.ADT
	ChnlPH  ph.ADT
	ChnlID  id.ADT
	StateID id.ADT
	Rev     rev.ADT
}

func ErrMissingChnl(want ph.ADT) error {
	return fmt.Errorf("channel missing in cfg: %v", want)
}

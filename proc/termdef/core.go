package termdef

import (
	"fmt"
	"log/slog"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/identity"
	"orglang/go-engine/adt/symbol"
	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/adt/uniqsym"
	"orglang/go-engine/proc/termexp"
)

type API interface {
	Create(DefSpec) (typesem.SemRef, error)
	Retrieve(identity.ADT) (DefRec, error)
}

type DefSpec struct {
	ProcQN uniqsym.ADT // or dec.ProcID
	ProcES termexp.ExpSpec
}

type DefRec struct {
	Ref typesem.SemRef
}

type DefSnap struct {
	Ref typesem.SemRef
}

type service struct {
	procDefs   Repo
	transactor db.Transactor
	log        *slog.Logger
}

// for compilation purposes
func newAPI() API {
	return new(service)
}

func newService(
	procs Repo,
	transactor db.Transactor,
	log *slog.Logger,
) *service {
	return &service{procs, transactor, log}
}

func (s *service) Create(spec DefSpec) (typesem.SemRef, error) {
	return typesem.SemRef{}, nil
}

func (s *service) Retrieve(recID identity.ADT) (DefRec, error) {
	return DefRec{}, nil
}

func ErrDoesNotExist(want identity.ADT) error {
	return fmt.Errorf("rec doesn't exist: %v", want)
}

func ErrMissingInCfg(want symbol.ADT) error {
	return fmt.Errorf("channel missing in cfg: %v", want)
}

func ErrMissingInCfg2(want identity.ADT) error {
	return fmt.Errorf("channel missing in cfg: %v", want)
}

func ErrMissingInCtx(want symbol.ADT) error {
	return fmt.Errorf("channel missing in ctx: %v", want)
}

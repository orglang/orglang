package typedef

import (
	"context"
	"fmt"
	"log/slog"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/descsem"
	"orglang/go-engine/adt/seqnum"
	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/adt/uniqsym"
	"orglang/go-engine/adt/valkey"

	"orglang/go-engine/pool/typeexp"
)

type API interface {
	Create(DefSpec) (typesem.SemRef, error)
}

type DefSpec struct {
	TypeQN  uniqsym.ADT
	TypeExp typeexp.ExpSpec
}

type DefRec struct {
	TypeRef typesem.SemRef
	ExpVK   valkey.ADT
}

type DefSnap struct {
	TypeRef typesem.SemRef
	TypeExp typeexp.ExpSpec
}

type service struct {
	typeDefRepo Repo
	typeExpRepo typeexp.Repo
	descSemRepo descsem.Repo
	transactor  db.Transactor
	log         *slog.Logger
}

// for compilation purposes
func newAPI() API {
	return new(service)
}

func newService(
	typeDefRepo Repo,
	typeExpRepo typeexp.Repo,
	descSemRepo descsem.Repo,
	transactor db.Transactor,
	log *slog.Logger,
) *service {
	return &service{typeDefRepo, typeExpRepo, descSemRepo, transactor, log}
}

func (s *service) Create(spec DefSpec) (_ typesem.SemRef, err error) {
	ctx := context.Background()
	qnAttr := slog.Any("qn", spec.TypeQN)
	s.log.Debug("creation started", qnAttr, slog.Any("spec", spec))
	newExp, convErr := typeexp.ConvertSpecToRec(spec.TypeExp)
	if convErr != nil {
		return typesem.SemRef{}, convErr
	}
	newDef := DefRec{TypeRef: typesem.New(), ExpVK: newExp.Key()}
	newBind := descsem.SemRec{DescQN: spec.TypeQN, DescID: newDef.TypeRef.TypeID, Kind: descsem.TypeKind}
	transactErr := s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		err = s.descSemRepo.AddRec(uow, newBind)
		if err != nil {
			return err
		}
		err = s.typeExpRepo.AddRec(uow, newExp, newDef.TypeRef)
		if err != nil {
			return err
		}
		return s.typeDefRepo.AddRec(uow, newDef)
	})
	if transactErr != nil {
		s.log.Error("creation failed", qnAttr)
		return typesem.SemRef{}, transactErr
	}
	s.log.Debug("creation succeed", qnAttr, slog.Any("ref", newDef.TypeRef))
	return newDef.TypeRef, nil
}

func errOptimisticUpdate(got seqnum.ADT) error {
	return fmt.Errorf("entity concurrent modification: got revision %v", got)
}

func ErrMissingInEnv(want valkey.ADT) error {
	return fmt.Errorf("exp missing in env: %v", want)
}

package typedef

import (
	"context"
	"fmt"
	"iter"
	"log/slog"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/descsem"
	"orglang/go-engine/adt/identity"
	"orglang/go-engine/adt/seqnum"
	"orglang/go-engine/adt/symbol"
	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/adt/uniqsym"
	"orglang/go-engine/adt/valkey"
	"orglang/go-engine/proc/typeexp"
)

type API interface {
	Create(DefSpec) (DefSnap, error)
	Modify(DefSnap) (DefSnap, error)
	RetrieveSnap(typesem.SemRef) (DefSnap, error)
	retrieveSnap(DefRec) (DefSnap, error)
	RetreiveRefs() ([]typesem.SemRef, error)
}

type DefSpec struct {
	TypeQN  uniqsym.ADT
	TypeExp typeexp.ExpSpec
}

// aka TpDef
type DefRec struct {
	TypeRef typesem.SemRef
	ExpVK   valkey.ADT
}

type DefSnap struct {
	TypeRef typesem.SemRef
	DefSpec DefSpec
}

type Context struct {
	Assets map[symbol.ADT]typeexp.ExpRec
	Liabs  map[symbol.ADT]typeexp.ExpRec
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

func (s *service) Create(spec DefSpec) (_ DefSnap, err error) {
	ctx := context.Background()
	qnAttr := slog.Any("qn", spec.TypeQN)
	s.log.Debug("creation started", qnAttr, slog.Any("spec", spec))
	newExp, err := typeexp.ConvertSpecToRec(spec.TypeExp)
	if err != nil {
		return DefSnap{}, err
	}
	newDef := DefRec{TypeRef: typesem.New(), ExpVK: newExp.Key()}
	newDesc := descsem.SemRec{DescQN: spec.TypeQN, DescID: newDef.TypeRef.TypeID, Kind: descsem.TypeKind}
	err = s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		err = s.descSemRepo.AddRec(uow, newDesc)
		if err != nil {
			return err
		}
		err = s.typeExpRepo.AddRec(uow, newExp)
		if err != nil {
			return err
		}
		return s.typeDefRepo.AddRec(uow, newDef)
	})
	if err != nil {
		s.log.Error("creation failed", qnAttr)
		return DefSnap{}, err
	}
	s.log.Debug("creation succeed", qnAttr, slog.Any("ref", newDef.TypeRef))
	return DefSnap{
		TypeRef: newDef.TypeRef,
		DefSpec: spec,
	}, nil
}

func (s *service) Modify(snap DefSnap) (_ DefSnap, err error) {
	ctx := context.Background()
	refAttr := slog.Any("defRef", snap.TypeRef)
	s.log.Debug("modification started", refAttr)
	var rec DefRec
	err = s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		rec, err = s.typeDefRepo.GetRecByRef(uow, snap.TypeRef)
		return err
	})
	if err != nil {
		s.log.Error("modification failed", refAttr)
		return DefSnap{}, err
	}
	if snap.TypeRef.TypeRN != rec.TypeRef.TypeRN {
		s.log.Error("modification failed", refAttr)
		return DefSnap{}, errConcurrentModification(snap.TypeRef.TypeRN, rec.TypeRef.TypeRN)
	}
	snap.TypeRef.TypeRN = seqnum.Next(snap.TypeRef.TypeRN)
	curSnap, err := s.retrieveSnap(rec)
	if err != nil {
		s.log.Error("modification failed", refAttr)
		return DefSnap{}, err
	}
	err = s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		if typeexp.CheckSpec(snap.DefSpec.TypeExp, curSnap.DefSpec.TypeExp) != nil {
			newExp, err := typeexp.ConvertSpecToRec(snap.DefSpec.TypeExp)
			if err != nil {
				return err
			}
			err = s.typeExpRepo.AddRec(uow, newExp)
			if err != nil {
				return err
			}
			rec.ExpVK = newExp.Key()
			rec.TypeRef.TypeRN = snap.TypeRef.TypeRN
		}
		if rec.TypeRef.TypeRN == snap.TypeRef.TypeRN {
			err = s.typeDefRepo.ModifyRec(uow, rec)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		s.log.Error("modification failed", refAttr)
		return DefSnap{}, err
	}
	s.log.Debug("modification succeed", refAttr)
	return snap, nil
}

func (s *service) RetrieveSnap(ref typesem.SemRef) (_ DefSnap, err error) {
	ctx := context.Background()
	var rec DefRec
	getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		rec, err = s.typeDefRepo.GetRecByRef(uow, ref)
		return err
	})
	if getErr != nil {
		s.log.Error("retrieval failed", slog.Any("ref", ref))
		return DefSnap{}, getErr
	}
	return s.retrieveSnap(rec)
}

func (s *service) retrieveSnap(rec DefRec) (_ DefSnap, err error) { // revive:disable-line:confusing-naming
	ctx := context.Background()
	var expRec typeexp.ExpRec
	err = s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		expRec, err = s.typeExpRepo.SelectRecByVK(uow, rec.ExpVK)
		return err
	})
	if err != nil {
		s.log.Error("retrieval failed", slog.Any("ref", rec.TypeRef))
		return DefSnap{}, err
	}
	return DefSnap{
		TypeRef: rec.TypeRef,
		DefSpec: DefSpec{TypeExp: typeexp.ConvertRecToSpec(expRec)},
	}, nil
}

func (s *service) RetreiveRefs() (refs []typesem.SemRef, err error) {
	ctx := context.Background()
	err = s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		refs, err = s.typeDefRepo.GetRefs(uow)
		return err
	})
	if err != nil {
		s.log.Error("retrieval failed")
		return nil, err
	}
	return refs, nil
}

func CollectEnv(recs iter.Seq[DefRec]) []valkey.ADT {
	expIDs := []valkey.ADT{}
	for r := range recs {
		expIDs = append(expIDs, r.ExpVK)
	}
	return expIDs
}

func ErrSymMissingInEnv(want uniqsym.ADT) error {
	return fmt.Errorf("root missing in env: %v", want)
}

func errConcurrentModification(got seqnum.ADT, want seqnum.ADT) error {
	return fmt.Errorf("entity concurrent modification: want revision %v, got revision %v", want, got)
}

func errOptimisticUpdate(got seqnum.ADT) error {
	return fmt.Errorf("entity concurrent modification: got revision %v", got)
}

func ErrDoesNotExist(want identity.ADT) error {
	return fmt.Errorf("root doesn't exist: %v", want)
}

func ErrMissingInEnv(want valkey.ADT) error {
	return fmt.Errorf("exp missing in env: %v", want)
}

func ErrMissingInCfg(want identity.ADT) error {
	return fmt.Errorf("root missing in cfg: %v", want)
}

func ErrMissingInCtx(want symbol.ADT) error {
	return fmt.Errorf("root missing in ctx: %v", want)
}

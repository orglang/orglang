package termdec

import (
	"context"
	"fmt"
	"iter"
	"log/slog"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/identity"
	"orglang/go-engine/adt/termsem"
	"orglang/go-engine/adt/termvar"
	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/adt/uniqsym"

	"orglang/go-engine/proc/typedef"
)

type API interface {
	Incept(uniqsym.ADT) (termsem.SemRef, error)
	Create(DecSpec) (DecSnap, error)
	RetrieveSnap(termsem.SemRef) (DecSnap, error)
	RetreiveRefs() ([]termsem.SemRef, error)
}

type DecSpec struct {
	TermQN uniqsym.ADT
	// endpoint where process acts as a provider
	LiabVar termvar.VarSpec
	// endpoints where process acts as a client
	AssetVars []termvar.VarSpec
}

type DecRec struct {
	TermRef   termsem.SemRef
	TermQN    uniqsym.ADT
	LiabVar   termvar.VarRec
	AssetVars []termvar.VarRec
}

// aka ExpDec or ExpDecDef without expression
type DecSnap struct {
	TermRef   termsem.SemRef
	LiabVar   termvar.VarRec
	AssetVars []termvar.VarRec
}

type service struct {
	termDecRepo Repo
	typeDefRepo typedef.Repo
	typeSemRepo typesem.Repo
	transactor  db.Transactor
	log         *slog.Logger
}

// for compilation purposes
func newAPI() API {
	return new(service)
}

func newService(
	termDecRepo Repo,
	typeDefRepo typedef.Repo,
	typeSemRepo typesem.Repo,
	transactor db.Transactor,
	log *slog.Logger,
) *service {
	return &service{termDecRepo, typeDefRepo, typeSemRepo, transactor, log}
}

func (s *service) Incept(termQN uniqsym.ADT) (_ termsem.SemRef, err error) {
	ctx := context.Background()
	qnAttr := slog.Any("qn", termQN)
	s.log.Debug("inception started", qnAttr)
	newDec := DecRec{TermRef: termsem.New(), TermQN: termQN}
	err = s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		err = s.termDecRepo.AddRec(uow, newDec)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		s.log.Error("inception failed", qnAttr)
		return termsem.SemRef{}, err
	}
	s.log.Debug("inception succeed", qnAttr, slog.Any("ref", newDec.TermRef))
	return newDec.TermRef, nil
}

func (s *service) Create(spec DecSpec) (_ DecSnap, err error) {
	ctx := context.Background()
	qnAttr := slog.Any("qn", spec.TermQN)
	s.log.Debug("creation started", qnAttr, slog.Any("spec", spec))
	assetQNs := make([]uniqsym.ADT, 0, len(spec.AssetVars)+1)
	for _, spec := range spec.AssetVars {
		assetQNs = append(assetQNs, spec.TypeQN)
	}
	var typeRefs map[uniqsym.ADT]typesem.SemRef
	getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		typeRefs, err = s.typeSemRepo.GetRefsByQNs(uow, append(assetQNs, spec.LiabVar.TypeQN))
		return err
	})
	if getErr != nil {
		return DecSnap{}, getErr
	}
	newLiabVar := termvar.VarRec{
		TypeRef: typeRefs[spec.LiabVar.TypeQN],
		ChnlPH:  spec.LiabVar.ChnlPH,
	}
	newAssetVars := make([]termvar.VarRec, 0, len(spec.AssetVars))
	for _, assetVar := range spec.AssetVars {
		newAssetVars = append(newAssetVars, termvar.VarRec{
			TypeRef: typeRefs[assetVar.TypeQN],
			ChnlPH:  assetVar.ChnlPH,
		})
	}
	newDec := DecRec{TermRef: termsem.New(), TermQN: spec.TermQN, LiabVar: newLiabVar, AssetVars: newAssetVars}
	transactErr := s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		return s.termDecRepo.AddRec(uow, newDec)
	})
	if transactErr != nil {
		s.log.Error("creation failed", qnAttr)
		return DecSnap{}, transactErr
	}
	s.log.Debug("creation succeed", qnAttr, slog.Any("ref", newDec.TermRef))
	return DecSnap{TermRef: newDec.TermRef, LiabVar: newLiabVar, AssetVars: newAssetVars}, nil
}

func (s *service) RetrieveSnap(ref termsem.SemRef) (snap DecSnap, err error) {
	ctx := context.Background()
	err = s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		snap, err = s.termDecRepo.GetSnap(uow, ref)
		return err
	})
	if err != nil {
		s.log.Error("retrieval failed", slog.Any("ref", ref))
		return DecSnap{}, err
	}
	return snap, nil
}

func (s *service) RetreiveRefs() (refs []termsem.SemRef, err error) {
	ctx := context.Background()
	err = s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		refs, err = s.termDecRepo.GetRefs(uow)
		return err
	})
	if err != nil {
		s.log.Error("retrieval failed")
		return nil, err
	}
	return refs, nil
}

func CollectEnv(recs iter.Seq[DecRec]) []uniqsym.ADT {
	return []uniqsym.ADT{}
}

func ErrRootMissingInEnv(rid identity.ADT) error {
	return fmt.Errorf("root missing in env: %v", rid)
}

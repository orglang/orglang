package termdef

import (
	"context"
	"log/slog"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/descsem"
	"orglang/go-engine/adt/termsem"
	"orglang/go-engine/adt/termvar"
	"orglang/go-engine/adt/uniqsym"
	"orglang/go-engine/pool/typedef"
)

type API interface {
	Create(DefSpec) (termsem.SemRef, error)
}

// for compilation purposes
func newAPI() API {
	return new(service)
}

type DefSpec struct {
	TermQN    uniqsym.ADT
	LiabVar   termvar.VarSpec
	AssetVars []termvar.VarSpec
}

type DefRec struct {
	TermRef   termsem.SemRef
	LiabVar   termvar.VarRec
	AssetVars []termvar.VarRec
}

type DefSnap struct {
	TermRef termsem.SemRef
}

func newService(
	termDefRepo Repo,
	typeDefRepo typedef.Repo,
	descSemRepo descsem.Repo,
	transactor db.Transactor,
	log *slog.Logger,
) *service {
	return &service{termDefRepo, typeDefRepo, descSemRepo, transactor, log}
}

type service struct {
	termDefRepo Repo
	typeDefRepo typedef.Repo
	descSemRepo descsem.Repo
	transactor  db.Transactor
	log         *slog.Logger
}

func (s *service) Create(spec DefSpec) (_ termsem.SemRef, err error) {
	ctx := context.Background()
	qnAttr := slog.Any("qn", spec.TermQN)
	s.log.Debug("creation started", qnAttr, slog.Any("spec", spec))
	assetQNs := make([]uniqsym.ADT, 0, len(spec.AssetVars)+1)
	for _, varSpec := range spec.AssetVars {
		assetQNs = append(assetQNs, varSpec.TypeQN)
	}
	var typeDefs map[uniqsym.ADT]typedef.DefRec
	getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		typeDefs, err = s.typeDefRepo.GetRecsByQNs(uow, append(assetQNs, spec.LiabVar.TypeQN))
		return err
	})
	if getErr != nil {
		return termsem.SemRef{}, getErr
	}
	newLiabVar := termvar.VarRec{
		TypeRef: typeDefs[spec.LiabVar.TypeQN].TypeRef,
		ChnlPH:  spec.LiabVar.ChnlPH,
		ExpVK:   typeDefs[spec.LiabVar.TypeQN].ExpVK,
	}
	newAssetVars := make([]termvar.VarRec, 0, len(spec.AssetVars))
	for _, varSpec := range spec.AssetVars {
		newAssetVars = append(newAssetVars, termvar.VarRec{
			TypeRef: typeDefs[varSpec.TypeQN].TypeRef,
			ChnlPH:  varSpec.ChnlPH,
			ExpVK:   typeDefs[varSpec.TypeQN].ExpVK,
		})
	}
	newDef := DefRec{TermRef: termsem.New(), LiabVar: newLiabVar, AssetVars: newAssetVars}
	newBind := descsem.SemRec{DescQN: spec.TermQN, DescID: newDef.TermRef.TermID, Kind: descsem.TermKind}
	transactErr := s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		err = s.descSemRepo.AddRec(uow, newBind)
		if err != nil {
			return err
		}
		return s.termDefRepo.AddRec(uow, newDef)
	})
	if transactErr != nil {
		s.log.Error("creation failed", qnAttr)
		return termsem.SemRef{}, transactErr
	}
	s.log.Debug("creation succeed", qnAttr, slog.Any("ref", newDef.TermRef))
	return newDef.TermRef, nil
}

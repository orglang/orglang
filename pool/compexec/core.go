package compexec

import (
	"context"
	"log/slog"
	"reflect"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/commsem"
	"orglang/go-engine/adt/compsem"
	"orglang/go-engine/adt/identity"
	"orglang/go-engine/adt/implsem"
	"orglang/go-engine/adt/option"
	"orglang/go-engine/adt/seqnum"
	"orglang/go-engine/adt/symbol"
	"orglang/go-engine/adt/uniqsym"
	"orglang/go-engine/adt/valkey"

	"orglang/go-engine/pool/commexch"
	"orglang/go-engine/pool/commturn"
	"orglang/go-engine/pool/compstep"
	"orglang/go-engine/pool/compvar"
	"orglang/go-engine/pool/termdef"
	"orglang/go-engine/pool/termexp"
	"orglang/go-engine/pool/typeexp"

	proccompexec "orglang/go-engine/proc/compexec"
	proctermdef "orglang/go-engine/proc/termdef"
)

type API interface {
	Run(ExecSpec) (compsem.SemRef, error) // aka Create
	Take(compstep.StepSpec) error
	Spawn(compstep.StepSpec) (compsem.SemRef, error)
}

type ExecSpec struct {
	// ссылка на декларацию вновь создаваемого пула
	TermQN uniqsym.ADT
	// внутренняя и внешняя ссылки на вновь создаваемый пул
	LiabVar compvar.VarSpec
	// внутренние и внешние ссылки на ранее созданные пулы
	AssetVars []compvar.VarSpec
}

type ExecRec struct {
	CompRef  compsem.SemRef
	LiabMode compvar.Mode
}

type ExecMod struct {
	CompRef compsem.SemRef
	Vars    []compvar.VarRec
}

func (mod ExecMod) isEmpty() bool { return len(mod.Vars) == 0 }

type ExecEff struct {
	Steps []compstep.StepSpec
}

type ExecSnap1 struct {
	CompRef compsem.SemRef
	LiabVar compvar.VarRec
}

type ExecSnap2 struct {
	CompRef    compsem.SemRef
	LiabMode   compvar.Mode
	StructVars []compvar.StructRec
	LinearVars []compvar.LinearRec
}

type ExecSnap3 struct {
	CompRef    compsem.SemRef
	StructVars map[symbol.ADT]compvar.StructRec
	StructExps map[symbol.ADT]typeexp.ExpRec
	LinearVars map[symbol.ADT]compvar.LinearRec
	LinearExps map[symbol.ADT]typeexp.ExpRec
}

type service struct {
	compExecRepo   Repo
	compExecBroker Broker
	compVarRepo    compvar.Repo
	commExchRepo   commexch.Repo
	commTurnRepo   commturn.Repo
	typeExpRepo    typeexp.Repo
	procExecRepo   proccompexec.Repo
	termDefRepo    termdef.Repo
	implSemRepo    implsem.Repo
	compSemRepo    compsem.Repo
	transactor     db.Transactor
	log            *slog.Logger
}

// for compilation purposes
func newAPI() API {
	return new(service)
}

func newService(
	compExecRepo Repo,
	compExecExch Broker,
	compVarRepo compvar.Repo,
	commExchRepo commexch.Repo,
	commTurnRepo commturn.Repo,
	typeExpRepo typeexp.Repo,
	procExecRepo proccompexec.Repo,
	termDefRepo termdef.Repo,
	implSemRepo implsem.Repo,
	compSemRepo compsem.Repo,
	transactor db.Transactor,
	log *slog.Logger,
) *service {
	name := slog.String("name", reflect.TypeFor[service]().Name())
	return &service{
		compExecRepo, compExecExch, compVarRepo,
		commExchRepo, commTurnRepo, typeExpRepo, procExecRepo, termDefRepo,
		implSemRepo, compSemRepo,
		transactor, log.With(name),
	}
}

func (s *service) Run(spec ExecSpec) (_ compsem.SemRef, err error) {
	ctx := context.Background()
	specAttr := slog.Any("spec", spec)
	s.log.Debug("creation started", specAttr)
	var termDec termdef.DefRec
	getErr1 := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		termDec, err = s.termDefRepo.GetRecByQN(uow, spec.TermQN)
		return err
	})
	if getErr1 != nil {
		s.log.Error("creation failed", specAttr)
		return compsem.SemRef{}, getErr1
	}
	assetQNs := make([]uniqsym.ADT, 0, len(spec.AssetVars))
	for _, assetVar := range spec.AssetVars {
		if assetVar.TermQN == spec.LiabVar.TermQN {
			continue
		}
		assetQNs = append(assetQNs, assetVar.TermQN)
	}
	var assetExecs map[uniqsym.ADT]ExecSnap1
	getErr2 := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		assetExecs, err = s.compExecRepo.GetSnapMapByQNs(uow, assetQNs)
		return err
	})
	if getErr2 != nil {
		s.log.Error("creation failed", specAttr)
		return compsem.SemRef{}, getErr2
	}
	newExec := ExecRec{CompRef: compsem.New(), LiabMode: compvar.StructMode}
	newExch := commexch.ExchRec{CommRef: commsem.New(), OffsetNr: seqnum.Zero}
	newImpl := implsem.SemRec{ImplQN: spec.TermQN, ImplID: newExec.CompRef.CompID}
	newLiabVar := compvar.StructRec{
		CompRef: newExec.CompRef,
		CommRef: newExch.CommRef,
		ChnlID:  identity.New(),
		ChnlPH:  spec.LiabVar.ChnlPH,
		ChnlBS:  compvar.LiabSide,
		ExpVK:   termDec.LiabVar.ExpVK,
	}
	newAssetVars := make([]compvar.VarRec, 0, len(spec.AssetVars)+1)
	for _, assetVar := range spec.AssetVars {
		var commRef commsem.SemRef
		var chnlID identity.ADT
		var expVK valkey.ADT
		assetExec, ok := assetExecs[assetVar.TermQN]
		if !ok && assetVar.TermQN == spec.LiabVar.TermQN {
			commRef = newExch.CommRef
			chnlID = newLiabVar.ChnlID
			expVK = newLiabVar.ExpVK
		} else {
			commRef = assetExec.LiabVar.GetCommRef()
			chnlID = assetExec.LiabVar.GetChnlID()
			expVK = assetExec.LiabVar.GetExpVK()
		}
		newAssetVars = append(newAssetVars, compvar.StructRec{
			CompRef: newExec.CompRef,
			CommRef: commRef,
			ChnlID:  chnlID,
			ChnlPH:  assetVar.ChnlPH,
			ChnlBS:  compvar.AssetSide,
			ExpVK:   expVK,
		})
	}
	transactErr := s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		err = s.implSemRepo.AddRec(uow, newImpl)
		if err != nil {
			return err
		}
		err = s.compExecRepo.AddRec(uow, newExec)
		if err != nil {
			return err
		}
		err = s.compVarRepo.AddRecs(uow, append(newAssetVars, newLiabVar))
		if err != nil {
			return err
		}
		return s.commExchRepo.AddRec(uow, newExch)
	})
	if transactErr != nil {
		s.log.Error("creation failed", specAttr)
		return compsem.SemRef{}, transactErr
	}
	s.log.Debug("creation succeed", slog.Any("ref", newExec.CompRef))
	return newExec.CompRef, nil
}

func (s *service) Spawn(spec compstep.StepSpec) (_ compsem.SemRef, err error) {
	ctx := context.Background()
	refAttr := slog.Any("ref", spec.CompRef)
	s.log.Debug("proc spawning started", refAttr, slog.Any("exp", spec.PoolExp))
	newExec := proccompexec.ExecRec{CompRef: compsem.New(), LiabMode: compvar.LinearMode}
	transactErr := s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		return s.procExecRepo.AddRec(uow, newExec)
	})
	if transactErr != nil {
		s.log.Error("proc spawning failed", refAttr)
		return compsem.SemRef{}, transactErr
	}
	s.log.Debug("proc spawning succeed", refAttr, slog.Any("proc", newExec.CompRef))
	return newExec.CompRef, nil
}

func (s *service) Take(spec compstep.StepSpec) (err error) {
	ctx := context.Background()
	refAttr := slog.Any("ref", spec.CompRef)
	s.log.Debug("step taking started", refAttr, slog.Any("exp", spec.PoolExp))
	execSnap, retErr := s.retrieveSnap(spec.CompRef)
	if retErr != nil {
		s.log.Error("step taking failed", refAttr)
		return retErr
	}
	execMod, execEff, exchMod, takeErr := s.take(execSnap, spec.PoolExp)
	if takeErr != nil {
		s.log.Error("step taking failed", refAttr)
		return takeErr
	}
	transactErr := s.transactor.ExplicitTx(ctx, func(uow db.UoW) error {
		err = s.commTurnRepo.AddRecs(uow, exchMod.Turns)
		if err != nil {
			return err
		}
		err = s.commExchRepo.ModifyRec(uow, exchMod)
		if err != nil {
			return err
		}
		err = s.compVarRepo.AddRecs(uow, execMod.Vars)
		if err != nil {
			return err
		}
		return s.compSemRepo.TouchRef(uow, execSnap.CompRef)
	})
	if transactErr != nil {
		s.log.Error("step taking failed", refAttr)
		return transactErr
	}
	for _, step := range execEff.Steps {
		sendErr := s.compExecBroker.SendSpec(step)
		if sendErr != nil {
			s.log.Error("step taking failed", refAttr)
			return sendErr
		}
	}
	return nil
}

func (s *service) take(
	execSnap ExecSnap3,
	exp termexp.ExpSpec,
) (
	execMod ExecMod,
	execEff ExecEff,
	exchMod commexch.ExchMod,
	err error,
) {
	ctx := context.Background()
	compAttr := slog.Any("compRef", execSnap.CompRef)
	switch termExp := exp.(type) {
	case termexp.AcceptSpec:
		commChnl, ok := execSnap.StructVars[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCfg(termExp.CommChnlPH)
		}
		// вычисляем следующее состояние
		typeExp, ok := execSnap.StructExps[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCtx(termExp.CommChnlPH)
		}
		nextExpVK := typeExp.(typeexp.ProdRec).Next()
		// получаем снепшот коммуникации
		var commSnap commexch.ExchSnap
		getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(uow, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		exchMod.CommRef = commSnap.CommRef
		commAttr := slog.Any("commRef", commSnap.CommRef)
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку доступодателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.AcceptRec{
					ContChnlPH: commChnl.ChnlPH,
					ContExp:    termExp.ContExp,
				},
			})
			s.log.Debug("taking half done", compAttr, commAttr)
			return execMod, execEff, exchMod, nil
		}
		acquisition, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		// вяжем продолжение доступодателя
		execMod.Vars = append(execMod.Vars, compvar.LinearRec{
			CompRef: commChnl.CompRef,
			CommRef: commChnl.CommRef,
			ChnlID:  newChnlID,
			ChnlPH:  commChnl.ChnlPH,
			ChnlBS:  commChnl.ChnlBS,
			ExpVK:   nextExpVK,
		})
		if termExp.ContExp != nil {
			// шедулим продолжение доступодателя
			execEff.Steps = append(execEff.Steps, compstep.StepSpec{
				CompRef: execSnap.CompRef,
				PoolExp: termExp.ContExp,
			})
		}
		switch expRec := acquisition.ContExp.(type) {
		case termexp.AcquireRec:
			// сдвигаем офсет коммуникации
			exchMod.OffsetNr = option.Some(acquisition.CommRef.CommRN)
			// вяжем продолжение доступополучателя
			execMod.Vars = append(execMod.Vars, compvar.LinearRec{
				CompRef: acquisition.CompRef,
				CommRef: acquisition.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.ContChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			if expRec.ContExp != nil {
				// шедулим продолжение доступополучателя
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: acquisition.CompRef,
					PoolExp: expRec.ContExp,
				})
			}
		default:
			panic(termexp.ErrRecTypeUnexpected(acquisition.ContExp))
		}
		s.log.Debug("step taking succeed", compAttr, commAttr)
		return execMod, execEff, exchMod, nil
	case termexp.AcquireSpec:
		commChnl, ok := execSnap.StructVars[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCfg(termExp.CommChnlPH)
		}
		// вычисляем следующее состояние
		typeExp, ok := execSnap.StructExps[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCtx(termExp.CommChnlPH)
		}
		nextExpVK := typeExp.(typeexp.ProdRec).Next()
		// получаем снепшот коммуникации
		var commSnap commexch.ExchSnap
		getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(uow, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		exchMod.CommRef = commSnap.CommRef
		commAttr := slog.Any("commRef", commSnap.CommRef)
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку доступополучателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.AcquireRec{
					ContChnlPH: commChnl.ChnlPH,
					ContExp:    termExp.ContExp,
				},
			})
			s.log.Debug("taking half done", compAttr, commAttr)
			return execMod, execEff, exchMod, nil
		}
		acception, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		// вяжем продолжение доступополучателя
		execMod.Vars = append(execMod.Vars, compvar.LinearRec{
			CompRef: commChnl.CompRef,
			CommRef: commChnl.CommRef,
			ChnlID:  newChnlID,
			ChnlPH:  commChnl.ChnlPH,
			ChnlBS:  commChnl.ChnlBS,
			ExpVK:   nextExpVK,
		})
		if termExp.ContExp != nil {
			// шедулим продолжение доступополучателя
			execEff.Steps = append(execEff.Steps, compstep.StepSpec{
				CompRef: execSnap.CompRef,
				PoolExp: termExp.ContExp,
			})
		}
		switch expRec := acception.ContExp.(type) {
		case termexp.AcceptRec:
			// сдвигаем офсет коммуникации
			exchMod.OffsetNr = option.Some(acception.CommRef.CommRN)
			// вяжем продолжение доступодателя
			execMod.Vars = append(execMod.Vars, compvar.LinearRec{
				CompRef: acception.CompRef,
				CommRef: acception.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.ContChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			if expRec.ContExp != nil {
				// шедулим продолжение доступодателя
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: acception.CompRef,
					PoolExp: expRec.ContExp,
				})
			}
		default:
			panic(termexp.ErrRecTypeUnexpected(acception.ContExp))
		}
		s.log.Debug("step taking succeed", compAttr, commAttr)
		return execMod, execEff, exchMod, nil
	case termexp.ApplySpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCfg(termExp.CommChnlPH)
		}
		// вычисляем следующее состояние
		typeExp, ok := execSnap.LinearExps[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCtx(termExp.CommChnlPH)
		}
		nextExpVK := typeExp.(typeexp.ProdRec).Next()
		// получаем снепшот коммуникации
		var commSnap commexch.ExchSnap
		getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(uow, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		exchMod.CommRef = commSnap.CommRef
		commAttr := slog.Any("commRef", commSnap.CommRef)
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку соискателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.ApplyRec{
					ContChnlPH: commChnl.ChnlPH,
					ProcTermQN: termExp.ProcTermQN,
					ContExp:    termExp.ContExp,
				},
			})
			s.log.Debug("taking half done", compAttr, commAttr)
			return execMod, execEff, exchMod, nil
		}
		hiring, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		// вяжем продолжение соискателя
		execMod.Vars = append(execMod.Vars, compvar.LinearRec{
			CompRef: commChnl.CompRef,
			CommRef: commChnl.CommRef,
			ChnlID:  newChnlID,
			ChnlPH:  commChnl.ChnlPH,
			ChnlBS:  commChnl.ChnlBS,
			ExpVK:   nextExpVK,
		})
		if termExp.ContExp != nil {
			// шедулим продолжение соискателя
			execEff.Steps = append(execEff.Steps, compstep.StepSpec{
				CompRef: execSnap.CompRef,
				PoolExp: termExp.ContExp,
			})
		}
		switch expRec := hiring.ContExp.(type) {
		case termexp.HireRec:
			// вяжем продолжение нанимателя
			execMod.Vars = append(execMod.Vars, compvar.LinearRec{
				CompRef: hiring.CompRef,
				CommRef: hiring.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.ContChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			if expRec.ContExp != nil {
				// шедулим продолжение нанимателя
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: hiring.CompRef,
					PoolExp: expRec.ContExp,
				})
			}
		default:
			panic(termexp.ErrRecTypeUnexpected(hiring.ContExp))
		}
		s.log.Debug("step taking succeed", compAttr, commAttr)
		return execMod, execEff, exchMod, nil
	case termexp.HireSpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCfg(termExp.CommChnlPH)
		}
		// вычисляем следующее состояние
		typeExp, ok := execSnap.LinearExps[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCtx(termExp.CommChnlPH)
		}
		nextExpVK := typeExp.(typeexp.ProdRec).Next()
		// получаем снепшот коммуникации
		var commSnap commexch.ExchSnap
		getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(uow, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		exchMod.CommRef = commSnap.CommRef
		commAttr := slog.Any("commRef", commSnap.CommRef)
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку нанимателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.HireRec{
					ContChnlPH: commChnl.ChnlPH,
					ProcTermQN: termExp.ProcTermQN,
					ContExp:    termExp.ContExp,
				},
			})
			s.log.Debug("taking half done", compAttr, commAttr)
			return execMod, execEff, exchMod, nil
		}
		application, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		// вяжем продолжение нанимателя
		execMod.Vars = append(execMod.Vars, compvar.LinearRec{
			CompRef: commChnl.CompRef,
			CommRef: commChnl.CommRef,
			ChnlID:  newChnlID,
			ChnlPH:  commChnl.ChnlPH,
			ChnlBS:  commChnl.ChnlBS,
			ExpVK:   nextExpVK,
		})
		if termExp.ContExp != nil {
			// шедулим продолжение нанимателя
			execEff.Steps = append(execEff.Steps, compstep.StepSpec{
				CompRef: execSnap.CompRef,
				PoolExp: termExp.ContExp,
			})
		}
		switch expRec := application.ContExp.(type) {
		case termexp.ApplyRec:
			// вяжем продолжение соискателя
			execMod.Vars = append(execMod.Vars, compvar.LinearRec{
				CompRef: application.CompRef,
				CommRef: application.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.ContChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			if expRec.ContExp != nil {
				// запускаем продолжение соискателя
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: application.CompRef,
					PoolExp: expRec.ContExp,
				})
			}
		default:
			panic(termexp.ErrRecTypeUnexpected(application.ContExp))
		}
		s.log.Debug("step taking succeed", compAttr, commAttr)
		return execMod, execEff, exchMod, nil
	case termexp.ReleaseSpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCfg(termExp.CommChnlPH)
		}
		// вычисляем следующее состояние
		typeExp, ok := execSnap.LinearExps[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCtx(termExp.CommChnlPH)
		}
		nextExpVK := typeExp.(typeexp.ProdRec).Next()
		// получаем снепшот коммуникации
		var commSnap commexch.ExchSnap
		getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(uow, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		exchMod.CommRef = commSnap.CommRef
		commAttr := slog.Any("commRef", commSnap.CommRef)
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку доступовозвращателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.ReleaseRec{
					ContChnlPH: commChnl.ChnlPH,
				},
			})
			s.log.Debug("taking half done", compAttr, commAttr)
			return execMod, execEff, exchMod, nil
		}
		detaching, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		// вяжем продолжение доступовозвращателя
		execMod.Vars = append(execMod.Vars, compvar.LinearRec{
			CompRef: commChnl.CompRef,
			CommRef: commChnl.CommRef,
			ChnlID:  newChnlID,
			ChnlPH:  commChnl.ChnlPH,
			ChnlBS:  commChnl.ChnlBS,
			ExpVK:   nextExpVK,
		})
		switch expRec := detaching.ContExp.(type) {
		case termexp.DetachRec:
			// вяжем продолжение доступопринимателя
			execMod.Vars = append(execMod.Vars, compvar.LinearRec{
				CompRef: detaching.CompRef,
				CommRef: detaching.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.ContChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
		default:
			panic(termexp.ErrRecTypeUnexpected(detaching.ContExp))
		}
		s.log.Debug("step taking succeed", compAttr, commAttr)
		return execMod, execEff, exchMod, nil
	case termexp.DetachSpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCfg(termExp.CommChnlPH)
		}
		// вычисляем следующее состояние
		typeExp, ok := execSnap.LinearExps[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, proctermdef.ErrMissingInCtx(termExp.CommChnlPH)
		}
		nextExpVK := typeExp.(typeexp.ProdRec).Next()
		// получаем снепшот коммуникации
		var commSnap commexch.ExchSnap
		getErr := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(uow, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		exchMod.CommRef = commSnap.CommRef
		commAttr := slog.Any("commRef", commSnap.CommRef)
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку доступопринимателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.DetachRec{
					ContChnlPH: commChnl.ChnlPH,
				},
			})
			s.log.Debug("taking half done", compAttr, commAttr)
			return execMod, execEff, exchMod, nil
		}
		releasing, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		// вяжем продолжение доступопринимателя
		execMod.Vars = append(execMod.Vars, compvar.LinearRec{
			CompRef: commChnl.CompRef,
			CommRef: commChnl.CommRef,
			ChnlID:  newChnlID,
			ChnlPH:  commChnl.ChnlPH,
			ChnlBS:  commChnl.ChnlBS,
			ExpVK:   nextExpVK,
		})
		switch expRec := releasing.ContExp.(type) {
		case termexp.ReleaseRec:
			// вяжем продолжение доступовозвращателя
			execMod.Vars = append(execMod.Vars, compvar.LinearRec{
				CompRef: releasing.CompRef,
				CommRef: releasing.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.ContChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
		default:
			panic(termexp.ErrRecTypeUnexpected(releasing.ContExp))
		}
		s.log.Debug("step taking succeed", compAttr, commAttr)
		return execMod, execEff, exchMod, nil
	default:
		panic(termexp.ErrSpecTypeUnexpected(exp))
	}
}

func (s *service) retrieveSnap(ref compsem.SemRef) (_ ExecSnap3, err error) {
	ctx := context.Background()
	var execSnap ExecSnap2
	getErr1 := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		execSnap, err = s.compExecRepo.GetSnapByRef(uow, ref)
		return err
	})
	if getErr1 != nil {
		return ExecSnap3{}, getErr1
	}
	var structExps map[symbol.ADT]typeexp.ExpRec
	var linearExps map[symbol.ADT]typeexp.ExpRec
	getErr2 := s.transactor.ImplicitTx(ctx, func(uow db.UoW) error {
		structExps, err = s.typeExpRepo.GetRecMap(uow, ExtractExpVKs(execSnap.StructVars))
		if err != nil {
			return err
		}
		linearExps, err = s.typeExpRepo.GetRecMap(uow, ExtractExpVKs(execSnap.LinearVars))
		return err
	})
	if getErr2 != nil {
		return ExecSnap3{}, getErr2
	}
	return ExecSnap3{
		CompRef:    execSnap.CompRef,
		StructVars: compvar.ConvertRecsToRecMap(execSnap.StructVars),
		StructExps: structExps,
		LinearVars: compvar.ConvertRecsToRecMap(execSnap.LinearVars),
		LinearExps: linearExps,
	}, nil
}

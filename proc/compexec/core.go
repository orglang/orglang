package compexec

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"reflect"

	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/compsem"
	"orglang/go-engine/adt/compvar"
	"orglang/go-engine/adt/identity"
	"orglang/go-engine/adt/option"
	"orglang/go-engine/adt/polarity"
	"orglang/go-engine/adt/seqnum"
	"orglang/go-engine/adt/symbol"
	"orglang/go-engine/adt/uniqsym"
	"orglang/go-engine/adt/valkey"
	"orglang/go-engine/proc/commexch"
	"orglang/go-engine/proc/commturn"
	"orglang/go-engine/proc/compstep"
	"orglang/go-engine/proc/termdec"
	"orglang/go-engine/proc/termdef"
	"orglang/go-engine/proc/termexp"
	"orglang/go-engine/proc/typedef"
	"orglang/go-engine/proc/typeexp"
)

type API interface {
	Take(compstep.StepSpec) error
	RetrieveSnap(compsem.SemRef) (ExecSnap, error)
}

type ExecRec struct {
	CompRef  compsem.SemRef
	LiabMode compvar.Mode
}

type ExecMod struct {
	CompRefs   []compsem.SemRef
	LinearVars []compvar.LinearRec
}

type ExecEff struct {
	Steps []compstep.StepSpec
}

// aka Configuration
type ExecSnap struct {
	CompRef    compsem.SemRef
	LinearVars map[symbol.ADT]compvar.LinearRec
}

type Env struct {
	TypeDefs map[uniqsym.ADT]typedef.DefRec
	TypeExps map[valkey.ADT]typeexp.ExpRec
	ProcDecs map[identity.ADT]termdec.DecRec
}

func ChnlPH(rec compvar.LinearRec) symbol.ADT { return rec.ChnlPH }

type service struct {
	compExecRepo Repo
	commExchRepo commexch.Repo
	termDecRepo  termdec.Repo
	typeDefRepo  typedef.Repo
	typeExpRepo  typeexp.Repo
	operator     db.Operator
	log          *slog.Logger
}

// for compilation purposes
func newAPI() API {
	return new(service)
}

func newService(
	compExecRepo Repo,
	commExchRepo commexch.Repo,
	termDecRepo termdec.Repo,
	typeDefRepo typedef.Repo,
	typeExpRepo typeexp.Repo,
	operator db.Operator,
	log *slog.Logger,
) *service {
	name := slog.String("name", reflect.TypeFor[service]().Name())
	return &service{
		compExecRepo, commExchRepo,
		termDecRepo, typeDefRepo, typeExpRepo,
		operator, log.With(name),
	}
}

func (s *service) RetrieveSnap(ref compsem.SemRef) (_ ExecSnap, err error) {
	return ExecSnap{}, nil
}

func ErrMissingChnl(want symbol.ADT) error {
	return fmt.Errorf("channel missing in cfg: %v", want)
}

func (s *service) Take(spec compstep.StepSpec) (err error) {
	compAttr := slog.Any("proc", spec.CompRef)
	s.log.Debug("step taking started", compAttr, slog.Any("exp", spec.ProcExp))
	ctx := context.Background()
	// initial values
	compRef := spec.CompRef
	expSpec := spec.ProcExp
	for expSpec != nil {
		var execSnap ExecSnap
		getErr1 := s.operator.Implicit(ctx, func(ds db.Source) error {
			execSnap, err = s.compExecRepo.GetSnapByRef(ds, compRef)
			return err
		})
		if getErr1 != nil {
			s.log.Error("step taking failed", compAttr)
			return getErr1
		}
		if len(execSnap.LinearVars) == 0 {
			panic("zero channel binds")
		}
		decIDs := termexp.CollectEnv(expSpec)
		var procDecs map[identity.ADT]termdec.DecRec
		getErr2 := s.operator.Implicit(ctx, func(ds db.Source) error {
			procDecs, err = s.termDecRepo.SelectEnv(ds, decIDs)
			return err
		})
		if getErr2 != nil {
			s.log.Error("step taking failed", compAttr, slog.Any("decs", decIDs))
			return getErr2
		}
		typeQNs := termdec.CollectEnv(maps.Values(procDecs))
		var typeDefs map[uniqsym.ADT]typedef.DefRec
		getErr3 := s.operator.Implicit(ctx, func(ds db.Source) error {
			typeDefs, err = s.typeDefRepo.SelectEnv(ds, typeQNs)
			return err
		})
		if getErr3 != nil {
			s.log.Error("step taking failed", compAttr, slog.Any("types", typeQNs))
			return getErr3
		}
		envIDs := typedef.CollectEnv(maps.Values(typeDefs))
		ctxIDs := CollectCtx(maps.Values(execSnap.LinearVars))
		var typeExps map[valkey.ADT]typeexp.ExpRec
		getErr4 := s.operator.Implicit(ctx, func(ds db.Source) error {
			typeExps, err = s.typeExpRepo.SelectEnv(ds, append(envIDs, ctxIDs...))
			return err
		})
		if getErr4 != nil {
			s.log.Error("step taking failed", compAttr, slog.Any("env", envIDs), slog.Any("ctx", ctxIDs))
			return getErr4
		}
		procEnv := Env{ProcDecs: procDecs, TypeDefs: typeDefs, TypeExps: typeExps}
		procCtx := convertToCtx(maps.Values(execSnap.LinearVars), typeExps)
		// type checking
		err = s.checkType(procEnv, procCtx, execSnap, expSpec)
		if err != nil {
			s.log.Error("step taking failed", compAttr)
			return err
		}
		// step taking
		execMod, execEff, _, err := s.takeWith(procEnv, execSnap, expSpec)
		if err != nil {
			s.log.Error("step taking failed", compAttr)
			return err
		}
		err = s.operator.Explicit(ctx, func(ds db.Source) error {
			err = s.compExecRepo.ModifyRec(ds, execMod)
			if err != nil {
				s.log.Error("step taking failed", compAttr)
				return err
			}
			return nil
		})
		if err != nil {
			s.log.Error("step taking failed", compAttr)
			return err
		}
		// next values
		if len(execEff.Steps) > 0 {
			nextSpec := execEff.Steps[len(execEff.Steps)-1]
			compRef = nextSpec.CompRef
			expSpec = nextSpec.ProcExp
		}
	}
	s.log.Debug("step taking succeed", compAttr)
	return nil
}

func (s *service) takeWith(
	procEnv Env,
	execSnap ExecSnap,
	exp termexp.ExpSpec,
) (
	execMod ExecMod,
	execEff ExecEff,
	exchMod commexch.ExchMod,
	err error,
) {
	ctx := context.Background()
	compAttr := slog.Any("compRef", execSnap.CompRef)
	execMod.CompRefs = append(execMod.CompRefs, execSnap.CompRef)
	switch termExp := exp.(type) {
	case termexp.CloseSpec:
		commChnl, ok := execSnap.LinearVars[termExp.ContChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, termdef.ErrMissingInCfg(termExp.ContChnlPH)
		}
		commAttr := slog.Any("commRef", commChnl.CommRef)
		// получаем снепшот коммуникации
		var commSnap commexch.ExchSnap
		getErr := s.operator.Implicit(ctx, func(ds db.Source) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(ds, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr, commAttr)
			return execMod, execEff, exchMod, getErr
		}
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку закрывателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.CloseRec(termExp),
			})
			s.log.Debug("taking half done", compAttr, commAttr)
			return execMod, execEff, exchMod, nil
		}
		observation, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		nextExpVK := valkey.One.Invert()
		switch expRec := observation.ContExp.(type) {
		case termexp.WaitRec:
			// лишаем продолжения закрывателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: commChnl.CompRef,
				CommRef: commChnl.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  commChnl.ChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			// лишаем продолжения наблюдателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: observation.CompRef,
				CommRef: observation.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.ContChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			if expRec.ContExp != nil {
				// шедулим продолжение наблюдателя
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: observation.CompRef,
					ProcExp: expRec.ContExp,
				})
			}
			s.log.Debug("step taking succeed", compAttr, commAttr)
			return execMod, execEff, exchMod, nil
		default:
			panic(termexp.ErrRecTypeUnexpected(observation.ContExp))
		}
	case termexp.WaitSpec:
		commChnl, ok := execSnap.LinearVars[termExp.ContChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, termdef.ErrMissingInCfg(termExp.ContChnlPH)
		}
		// получаем снепшот соединения
		var commSnap commexch.ExchSnap
		getErr := s.operator.Implicit(ctx, func(ds db.Source) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(ds, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку наблюдателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.WaitRec(termExp),
			})
			s.log.Debug("taking half done", compAttr)
			return execMod, execEff, exchMod, nil
		}
		closage, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		nextExpVK := valkey.One.Invert()
		switch expRec := closage.ContExp.(type) {
		case termexp.CloseRec:
			// лишаем продолжения наблюдателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: commChnl.CompRef,
				CommRef: commChnl.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  commChnl.ChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			// лишаем продолжения закрывателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: closage.CompRef,
				CommRef: closage.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.ContChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			if termExp.ContExp != nil {
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: execSnap.CompRef,
					ProcExp: termExp.ContExp,
				})
			}
			s.log.Debug("step taking succeed", compAttr)
			return execMod, execEff, exchMod, nil
		case termexp.FwdRec:
			// перенаправляем продолжение наблюдателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: execSnap.CompRef,
				CommRef: commSnap.CommRef,
				ChnlID:  expRec.ContChnlID,
				ChnlPH:  commChnl.ChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   commChnl.ExpVK,
			})
			execEff.Steps = append(execEff.Steps, compstep.StepSpec{
				CompRef: execSnap.CompRef,
				ProcExp: termExp,
			})
			s.log.Debug("step taking succeed", compAttr)
			return execMod, execEff, exchMod, nil
		default:
			panic(termexp.ErrRecTypeUnexpected(closage.ContExp))
		}
	case termexp.SendSpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			err := termdef.ErrMissingInCfg(termExp.CommChnlPH)
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, err
		}
		typeExp, ok := procEnv.TypeExps[commChnl.ExpVK]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, typedef.ErrMissingInEnv(commChnl.ExpVK)
		}
		nextExpVK := typeExp.(typeexp.ProdRec).Next()
		valChnl, ok := execSnap.LinearVars[termExp.ValChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, termdef.ErrMissingInCfg(termExp.ValChnlPH)
		}
		// получаем снепшот соединения
		var commSnap commexch.ExchSnap
		getErr := s.operator.Implicit(ctx, func(ds db.Source) error {
			commSnap, err = s.commExchRepo.GetSnapByQry(ds, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		subscription := commSnap.NextTurn()
		if subscription == nil {
			// регистрируем подписку отправителя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.SendRec{
					CommChnlPH: commChnl.ChnlPH,
					ValChnlID:  valChnl.ChnlID,
					ValExpVK:   valChnl.ExpVK,
				},
			})
			s.log.Debug("taking half done", compAttr)
			return execMod, execEff, exchMod, nil
		}
		receival, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		newChnlID := identity.New()
		// вяжем продолжение отправителя
		execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
			CompRef: commChnl.CompRef,
			CommRef: commChnl.CommRef,
			ChnlID:  newChnlID,
			ChnlPH:  commChnl.ChnlPH,
			ChnlBS:  commChnl.ChnlBS,
			ExpVK:   nextExpVK,
		})
		// лишаем значения отправителя
		execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
			CompRef: commChnl.CompRef,
			CommRef: commChnl.CommRef,
			ChnlID:  identity.New(),
			ChnlPH:  valChnl.ChnlPH,
			ChnlBS:  valChnl.ChnlBS,
			ExpVK:   valChnl.ExpVK.Invert(),
		})
		switch expRec := receival.ContExp.(type) {
		case termexp.RecvRec:
			// вяжем продолжение принимателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: receival.CompRef,
				CommRef: receival.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  expRec.CommChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			// вяжем значение принимателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: receival.CompRef,
				CommRef: valChnl.CommRef,
				ChnlID:  valChnl.ChnlID,
				ChnlPH:  expRec.NewChnlPH,
				ChnlBS:  valChnl.ChnlBS,
				ExpVK:   valChnl.ExpVK,
			})
			if expRec.ContExp != nil {
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: receival.CompRef,
					ProcExp: expRec.ContExp,
				})
			}
			s.log.Debug("step taking succeed", compAttr)
			return execMod, execEff, exchMod, nil
		default:
			panic(termexp.ErrRecTypeUnexpected(receival.ContExp))
		}
	case termexp.RecvSpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, termdef.ErrMissingInCfg(termExp.CommChnlPH)
		}
		typeExp, ok := procEnv.TypeExps[commChnl.ExpVK]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, typedef.ErrMissingInEnv(commChnl.ExpVK)
		}
		nextExpVK := typeExp.(typeexp.ProdRec).Next()
		// получаем снепшот соединения
		var connSnap commexch.ExchSnap
		getErr := s.operator.Implicit(ctx, func(ds db.Source) error {
			connSnap, err = s.commExchRepo.GetSnapByQry(ds, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		publication := connSnap.NextTurn()
		if publication == nil {
			newChnlID := identity.New()
			// вяжем продолжение принимателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: commChnl.CompRef,
				CommRef: commChnl.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  commChnl.ChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			// регистрируем подписку принимателя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: connSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.RecvRec{
					CommChnlPH: commChnl.ChnlPH,
					ContChnlID: newChnlID,
					NewChnlPH:  termExp.NewChnlPH,
					ContExp:    termExp.ContExp,
				},
			})
			s.log.Debug("taking half done", compAttr)
			return execMod, execEff, exchMod, nil
		}
		sending, ok := publication.(commturn.PubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(publication))
		}
		switch valExp := sending.ValExp.(type) {
		case termexp.SendRec:
			// вяжем продолжение принимателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: commChnl.CompRef,
				CommRef: commChnl.CommRef,
				ChnlID:  valExp.ContChnlID,
				ChnlPH:  commChnl.ChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			// вяжем значение принимателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: commChnl.CompRef,
				CommRef: valExp.CommRef,
				ChnlID:  valExp.ValChnlID,
				ChnlPH:  termExp.NewChnlPH,
				ChnlBS:  compvar.AssetSide,
				ExpVK:   valExp.ValExpVK,
			})
			if termExp.ContExp != nil {
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: execSnap.CompRef,
					ProcExp: termExp.ContExp,
				})
			}
			s.log.Debug("step taking succeed", compAttr)
			return execMod, execEff, exchMod, nil
		default:
			panic(termexp.ErrRecTypeUnexpected(sending.ValExp))
		}
	case termexp.LabSpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			err := termdef.ErrMissingInCfg(termExp.CommChnlPH)
			s.log.Error("step taking failed")
			return execMod, execEff, exchMod, err
		}
		typeExp, ok := procEnv.TypeExps[commChnl.ExpVK]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, typedef.ErrMissingInEnv(commChnl.ExpVK)
		}
		nextExpVK := typeExp.(typeexp.SumRec).Next(termExp.ValLabQN)
		// получаем снепшот соединения
		var connSnap commexch.ExchSnap
		getErr := s.operator.Implicit(ctx, func(ds db.Source) error {
			connSnap, err = s.commExchRepo.GetSnapByQry(ds, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		subscription := connSnap.NextTurn()
		if subscription == nil {
			newChnlID := identity.New()
			// вяжем продолжение решателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: commChnl.CompRef,
				CommRef: commChnl.CommRef,
				ChnlID:  newChnlID,
				ChnlPH:  commChnl.ChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			// регистрируем сообщение решателя
			exchMod.Turns = append(exchMod.Turns, commturn.PubRec{
				CommRef: connSnap.CommRef,
				CompRef: execSnap.CompRef,
				ChnlID:  commChnl.ChnlID,
				ValExp: termexp.LabRec{
					CommChnlPH: commChnl.ChnlPH,
					ContChnlID: newChnlID,
					ValLabQN:   termExp.ValLabQN,
				},
			})
			s.log.Debug("taking half done", compAttr)
			return execMod, execEff, exchMod, nil
		}
		folowing, ok := subscription.(commturn.SubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(subscription))
		}
		switch contExp := folowing.ContExp.(type) {
		case termexp.CaseRec:
			// вяжем продолжение решателя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: commChnl.CompRef,
				CommRef: commChnl.CommRef,
				ChnlID:  contExp.ContChnlID,
				ChnlPH:  commChnl.ChnlPH,
				ChnlBS:  commChnl.ChnlBS,
				ExpVK:   nextExpVK,
			})
			// вяжем продолжение последователя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: folowing.CompRef,
				CommRef: folowing.CommRef,
				ChnlID:  contExp.ContChnlID,
				ChnlPH:  contExp.CommChnlPH,
				// TODO значение ChnlBS
				ExpVK: nextExpVK,
			})
			if contExp.ContExps[termExp.ValLabQN] != nil {
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: folowing.CompRef,
					ProcExp: contExp.ContExps[termExp.ValLabQN],
				})
			}
			s.log.Debug("step taking succeed", compAttr)
			return execMod, execEff, exchMod, nil
		default:
			panic(termexp.ErrRecTypeUnexpected(folowing.ContExp))
		}
	case termexp.CaseSpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			err := termdef.ErrMissingInCfg(termExp.CommChnlPH)
			s.log.Error("step taking failed")
			return execMod, execEff, exchMod, err
		}
		// получаем снепшот соединения
		var caseConnSnap commexch.ExchSnap
		getErr := s.operator.Implicit(ctx, func(ds db.Source) error {
			caseConnSnap, err = s.commExchRepo.GetSnapByQry(ds, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		publication := caseConnSnap.NextTurn()
		if publication == nil {
			newChnlID := identity.New()
			// регистрируем подписку последователя
			exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
				CommRef: commChnl.CommRef,
				CompRef: commChnl.CompRef,
				ChnlID:  commChnl.ChnlID,
				ContExp: termexp.CaseRec{
					CommChnlPH: commChnl.ChnlPH,
					ContChnlID: newChnlID,
					ContExps:   termExp.ContExps,
				},
			})
			s.log.Debug("taking half done", compAttr)
			return execMod, execEff, exchMod, nil
		}
		decision, ok := publication.(commturn.PubRec)
		if !ok {
			panic(commturn.ErrRecTypeUnexpected(publication))
		}
		switch valExp := decision.ValExp.(type) {
		case termexp.LabRec:
			// вяжем продолжение последователя
			execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
				CompRef: commChnl.CompRef,
				CommRef: commChnl.CommRef,
				ChnlID:  valExp.ContChnlID,
				ChnlPH:  commChnl.ChnlPH,
				// TODO значение ChnlBS
				ExpVK: valExp.ValExpVK,
			})
			if termExp.ContExps[valExp.ValLabQN] != nil {
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: execSnap.CompRef,
					ProcExp: termExp.ContExps[valExp.ValLabQN],
				})
			}
			s.log.Debug("step taking succeed", compAttr)
			return execMod, execEff, exchMod, nil
		default:
			panic(termexp.ErrRecTypeUnexpected(decision.ValExp))
		}
	case termexp.FwdSpec:
		commChnl, ok := execSnap.LinearVars[termExp.CommChnlPH]
		if !ok {
			s.log.Error("step taking failed")
			return execMod, execEff, exchMod, termdef.ErrMissingInCfg(termExp.CommChnlPH)
		}
		contChnl, ok := execSnap.LinearVars[termExp.ContChnlPH]
		if !ok {
			s.log.Error("step taking failed")
			return execMod, execEff, exchMod, termdef.ErrMissingInCfg(termExp.ContChnlPH)
		}
		typeExp, ok := procEnv.TypeExps[commChnl.ExpVK]
		if !ok {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, typedef.ErrMissingInEnv(commChnl.ExpVK)
		}
		// получаем снепшот соединения
		var fwdConnSnap commexch.ExchSnap
		getErr := s.operator.Implicit(ctx, func(ds db.Source) error {
			fwdConnSnap, err = s.commExchRepo.GetSnapByQry(ds, commexch.ExchQry{
				CommRef: commChnl.CommRef,
				ChnlID:  option.Some(commChnl.ChnlID),
			})
			return err
		})
		if getErr != nil {
			s.log.Error("step taking failed", compAttr)
			return execMod, execEff, exchMod, getErr
		}
		communication := fwdConnSnap.NextTurn()
		switch typeExp.Pol() {
		case polarity.Pos:
			switch forwardable := communication.(type) {
			case commturn.SubRec:
				// перенаправляем подписчика
				execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
					CompRef: forwardable.CompRef,
					CommRef: forwardable.CommRef,
					ChnlID:  commChnl.ChnlID,
					ChnlPH:  forwardable.ContExp.Via(),
					// TODO значение ChnlBS
					ExpVK: commChnl.ExpVK,
				})
				if forwardable.ContExp != nil {
					execEff.Steps = append(execEff.Steps, compstep.StepSpec{
						CompRef: forwardable.CompRef,
						ProcExp: forwardable.ContExp,
					})
				}
				s.log.Debug("step taking succeed", compAttr)
				return execMod, execEff, exchMod, nil
			case commturn.PubRec:
				// перенаправляем публикатора
				execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
					CompRef: forwardable.CompRef,
					CommRef: forwardable.CommRef,
					ChnlID:  contChnl.ChnlID,
					ChnlPH:  forwardable.ValExp.Via(),
					// TODO значение ChnlBS
					ExpVK: contChnl.ExpVK,
				})
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: forwardable.CompRef,
					ProcExp: forwardable.ValExp,
				})
				s.log.Debug("step taking succeed", compAttr)
				return execMod, execEff, exchMod, nil
			case nil:
				// лишаем значений схлопывающегося
				execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
					CompRef: commChnl.CompRef,
					CommRef: commChnl.CommRef,
					ChnlID:  commChnl.ChnlID,
					ChnlPH:  termExp.CommChnlPH,
					ChnlBS:  commChnl.ChnlBS,
					ExpVK:   commChnl.ExpVK.Invert(),
				})
				execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
					CompRef: commChnl.CompRef,
					// TODO значение CommRef
					ChnlID: contChnl.ChnlID,
					ChnlPH: termExp.ContChnlPH,
					ChnlBS: contChnl.ChnlBS,
					ExpVK:  contChnl.ExpVK.Invert(),
				})
				// регистрируем сообщение схлопывающегося
				exchMod.Turns = append(exchMod.Turns, commturn.PubRec{
					CommRef: fwdConnSnap.CommRef,
					CompRef: execSnap.CompRef,
					ChnlID:  commChnl.ChnlID,
					ValExp: termexp.FwdRec{
						ContChnlID: contChnl.ChnlID,
					},
				})
				s.log.Debug("taking half done", compAttr)
				return execMod, execEff, exchMod, nil
			default:
				panic(commturn.ErrRecTypeUnexpected(communication))
			}
		case polarity.Neg:
			switch forwardable := communication.(type) {
			case commturn.SubRec:
				// перенаправляем подписчика
				execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
					CompRef: forwardable.CompRef,
					CommRef: forwardable.CommRef,
					ChnlID:  contChnl.ChnlID,
					ChnlPH:  forwardable.ContExp.Via(),
					// TODO значение ChnlBS
					ExpVK: contChnl.ExpVK,
				})
				if forwardable.ContExp != nil {
					execEff.Steps = append(execEff.Steps, compstep.StepSpec{
						CompRef: forwardable.CompRef,
						ProcExp: forwardable.ContExp,
					})
				}
				s.log.Debug("step taking succeed", compAttr)
				return execMod, execEff, exchMod, nil
			case commturn.PubRec:
				// перенаправляем публикатора
				execMod.LinearVars = append(execMod.LinearVars, compvar.LinearRec{
					CompRef: forwardable.CompRef,
					CommRef: forwardable.CommRef,
					ChnlID:  commChnl.ChnlID,
					ChnlPH:  forwardable.ValExp.Via(),
					// TODO значение ChnlBS
					ExpVK: commChnl.ExpVK,
				})
				execEff.Steps = append(execEff.Steps, compstep.StepSpec{
					CompRef: forwardable.CompRef,
					ProcExp: forwardable.ValExp, // TODO: несовпадение типов
				})
				s.log.Debug("step taking succeed", compAttr)
				return execMod, execEff, exchMod, nil
			case nil:
				// регистрируем подписку схлопывающегося
				exchMod.Turns = append(exchMod.Turns, commturn.SubRec{
					CommRef: fwdConnSnap.CommRef,
					CompRef: commChnl.CompRef,
					ChnlID:  commChnl.ChnlID,
					ContExp: termexp.FwdRec{
						ContChnlID: contChnl.ChnlID,
					},
				})
				s.log.Debug("taking half done", compAttr)
				return execMod, execEff, exchMod, nil
			default:
				panic(commturn.ErrRecTypeUnexpected(communication))
			}
		default:
			panic(typeexp.ErrPolarityUnexpected(typeExp))
		}
	default:
		panic(termexp.ErrExpTypeUnexpected(exp))
	}
}

func CollectCtx(chnls iter.Seq[compvar.LinearRec]) []valkey.ADT {
	return nil
}

func convertToCtx(chnlBinds iter.Seq[compvar.LinearRec], typeExps map[valkey.ADT]typeexp.ExpRec) typedef.Context {
	assets := make(map[symbol.ADT]typeexp.ExpRec, 1)
	liabs := make(map[symbol.ADT]typeexp.ExpRec, 1)
	for bind := range chnlBinds {
		if bind.ChnlBS == compvar.LiabSide {
			liabs[bind.ChnlPH] = typeExps[bind.ExpVK]
		} else {
			assets[bind.ChnlPH] = typeExps[bind.ExpVK]
		}
	}
	return typedef.Context{Assets: assets, Liabs: liabs}
}

func (s *service) checkType(
	procEnv Env,
	procCtx typedef.Context,
	execSnap ExecSnap,
	expSpec termexp.ExpSpec,
) error {
	chnlBR, ok := execSnap.LinearVars[expSpec.Via()]
	if !ok {
		panic("no comm chnl in proc snap")
	}
	if chnlBR.ChnlBS == compvar.LiabSide {
		return s.checkProvider(procEnv, procCtx, execSnap, expSpec)
	}
	return s.checkClient(procEnv, procCtx, execSnap, expSpec)
}

func (s *service) checkProvider(
	procEnv Env,
	procCtx typedef.Context,
	procCfg ExecSnap,
	es termexp.ExpSpec,
) error {
	switch expSpec := es.(type) {
	case termexp.CloseSpec:
		// check ctx
		if len(procCtx.Assets) > 0 {
			err := fmt.Errorf("context mismatch: want 0 items, got %v items", len(procCtx.Assets))
			s.log.Error("checking failed")
			return err
		}
		// check via
		gotVia, ok := procCtx.Liabs[expSpec.ContChnlPH]
		if !ok {
			err := typedef.ErrMissingInCtx(expSpec.ContChnlPH)
			s.log.Error("checking failed")
			return err
		}
		err := typeexp.CheckRec(gotVia, typeexp.OneRec{})
		if err != nil {
			s.log.Error("checking failed")
			return err
		}
		// no cont to check
		delete(procCtx.Liabs, expSpec.ContChnlPH)
		return nil
	case termexp.WaitSpec:
		err := termexp.ErrExpTypeMismatch(es, termexp.CloseSpec{})
		s.log.Error("checking failed")
		return err
	case termexp.SendSpec:
		// check via
		gotVia, ok := procCtx.Liabs[expSpec.CommChnlPH]
		if !ok {
			err := typedef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.TensorRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check value
		gotVal, ok := procCtx.Assets[expSpec.ValChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.ValChnlPH)
			s.log.Error("checking failed")
			return err
		}
		err := typeexp.CheckRec(gotVal, wantVia.Val)
		if err != nil {
			s.log.Error("checking failed")
			return err
		}
		// no cont to check
		procCtx.Liabs[expSpec.CommChnlPH] = wantVia.Cont
		delete(procCtx.Assets, expSpec.ValChnlPH)
		return nil
	case termexp.RecvSpec:
		// check via
		gotVia, ok := procCtx.Liabs[expSpec.CommChnlPH]
		if !ok {
			err := typedef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.LolliRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check value
		gotVal, ok := procCtx.Assets[expSpec.NewChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.NewChnlPH)
			s.log.Error("checking failed")
			return err
		}
		err := typeexp.CheckRec(gotVal, wantVia.Val)
		if err != nil {
			s.log.Error("checking failed")
			return err
		}
		// check cont
		procCtx.Liabs[expSpec.CommChnlPH] = wantVia.Cont
		procCtx.Assets[expSpec.NewChnlPH] = wantVia.Val
		return s.checkType(procEnv, procCtx, procCfg, expSpec.ContExp)
	case termexp.LabSpec:
		// check via
		gotVia, ok := procCtx.Liabs[expSpec.CommChnlPH]
		if !ok {
			err := typedef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.PlusRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check label
		choice, ok := wantVia.Choices[expSpec.ValLabQN]
		if !ok {
			err := fmt.Errorf("label mismatch: want %v, got %v", maps.Keys(wantVia.Choices), expSpec.ValLabQN)
			s.log.Error("checking failed")
			return err
		}
		// no cont to check
		procCtx.Liabs[expSpec.CommChnlPH] = choice
		return nil
	case termexp.CaseSpec:
		// check via
		gotVia, ok := procCtx.Liabs[expSpec.CommChnlPH]
		if !ok {
			err := typedef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.WithRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check conts
		if len(expSpec.ContExps) != len(wantVia.Choices) {
			err := fmt.Errorf("state mismatch: want %v choices, got %v conts", len(wantVia.Choices), len(expSpec.ContExps))
			s.log.Error("checking failed")
			return err
		}
		for label, choice := range wantVia.Choices {
			cont, ok := expSpec.ContExps[label]
			if !ok {
				err := fmt.Errorf("label mismatch: want %v, got nothing", label)
				s.log.Error("checking failed")
				return err
			}
			procCtx.Liabs[expSpec.CommChnlPH] = choice
			err := s.checkType(procEnv, procCtx, procCfg, cont)
			if err != nil {
				s.log.Error("checking failed")
				return err
			}
		}
		return nil
	case termexp.FwdSpec:
		if len(procCtx.Assets) != 1 {
			err := fmt.Errorf("context mismatch: want 1 item, got %v items", len(procCtx.Assets))
			s.log.Error("checking failed")
			return err
		}
		viaSt, ok := procCtx.Liabs[expSpec.CommChnlPH]
		if !ok {
			err := typedef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		fwdSt, ok := procCtx.Assets[expSpec.ContChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.ContChnlPH)
			s.log.Error("checking failed")
			return err
		}
		if fwdSt.Pol() != viaSt.Pol() {
			err := typeexp.ErrPolarityMismatch(fwdSt, viaSt)
			s.log.Error("checking failed")
			return err
		}
		err := typeexp.CheckRec(fwdSt, viaSt)
		if err != nil {
			s.log.Error("checking failed")
			return err
		}
		delete(procCtx.Liabs, expSpec.CommChnlPH)
		delete(procCtx.Assets, expSpec.ContChnlPH)
		return nil
	default:
		panic(termexp.ErrExpTypeUnexpected(es))
	}
}

func (s *service) checkClient(
	procEnv Env,
	procCtx typedef.Context,
	procCfg ExecSnap,
	es termexp.ExpSpec,
) error {
	switch expSpec := es.(type) {
	case termexp.CloseSpec:
		err := termexp.ErrExpTypeMismatch(es, termexp.WaitSpec{})
		s.log.Error("checking failed")
		return err
	case termexp.WaitSpec:
		// check via
		gotVia, ok := procCtx.Assets[expSpec.ContChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.ContChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.OneRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check cont
		delete(procCtx.Assets, expSpec.ContChnlPH)
		return s.checkType(procEnv, procCtx, procCfg, expSpec.ContExp)
	case termexp.SendSpec:
		// check via
		gotVia, ok := procCtx.Assets[expSpec.CommChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.LolliRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check value
		gotVal, ok := procCtx.Assets[expSpec.ValChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.ValChnlPH)
			s.log.Error("checking failed")
			return err
		}
		err := typeexp.CheckRec(gotVal, wantVia.Val)
		if err != nil {
			s.log.Error("checking failed")
			return err
		}
		procCtx.Assets[expSpec.CommChnlPH] = wantVia.Cont
		delete(procCtx.Assets, expSpec.ValChnlPH)
		return nil
	case termexp.RecvSpec:
		// check via
		gotVia, ok := procCtx.Assets[expSpec.CommChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.TensorRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check value
		gotVal, ok := procCtx.Assets[expSpec.NewChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.NewChnlPH)
			s.log.Error("checking failed")
			return err
		}
		err := typeexp.CheckRec(gotVal, wantVia.Val)
		if err != nil {
			s.log.Error("checking failed")
			return err
		}
		// check cont
		procCtx.Assets[expSpec.CommChnlPH] = wantVia.Cont
		procCtx.Assets[expSpec.NewChnlPH] = wantVia.Val
		return s.checkType(procEnv, procCtx, procCfg, expSpec.ContExp)
	case termexp.LabSpec:
		// check via
		gotVia, ok := procCtx.Assets[expSpec.CommChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.WithRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check label
		choice, ok := wantVia.Choices[expSpec.ValLabQN]
		if !ok {
			err := fmt.Errorf("label mismatch: want %v, got %v", maps.Keys(wantVia.Choices), expSpec.ValLabQN)
			s.log.Error("checking failed")
			return err
		}
		procCtx.Assets[expSpec.CommChnlPH] = choice
		return nil
	case termexp.CaseSpec:
		// check via
		gotVia, ok := procCtx.Assets[expSpec.CommChnlPH]
		if !ok {
			err := termdef.ErrMissingInCtx(expSpec.CommChnlPH)
			s.log.Error("checking failed")
			return err
		}
		wantVia, ok := gotVia.(typeexp.PlusRec)
		if !ok {
			err := typeexp.ErrSnapTypeMismatch(gotVia, wantVia)
			s.log.Error("checking failed")
			return err
		}
		// check conts
		if len(expSpec.ContExps) != len(wantVia.Choices) {
			err := fmt.Errorf("state mismatch: want %v choices, got %v conts", len(wantVia.Choices), len(expSpec.ContExps))
			s.log.Error("checking failed")
			return err
		}
		for label, choice := range wantVia.Choices {
			cont, ok := expSpec.ContExps[label]
			if !ok {
				err := fmt.Errorf("label mismatch: want %v, got nothing", label)
				s.log.Error("checking failed")
				return err
			}
			procCtx.Assets[expSpec.CommChnlPH] = choice
			err := s.checkType(procEnv, procCtx, procCfg, cont)
			if err != nil {
				s.log.Error("checking failed")
				return err
			}
		}
		return nil
	// case procexp.SpawnSpecOld:
	// 	procDec, ok := procEnv.ProcDecs[expSpec.SigID]
	// 	if !ok {
	// 		err := procdec.ErrRootMissingInEnv(expSpec.SigID)
	// 		s.log.Error("checking failed")
	// 		return err
	// 	}
	// 	// check vals
	// 	if len(expSpec.Ys) != len(procDec.ClientVRs) {
	// 		err := fmt.Errorf("context mismatch: want %v items, got %v items", len(procDec.ClientVRs), len(expSpec.Ys))
	// 		s.log.Error("checking failed", slog.Any("want", procDec.ClientVRs), slog.Any("got", expSpec.Ys))
	// 		return err
	// 	}
	// 	if len(expSpec.Ys) == 0 {
	// 		return nil
	// 	}
	// 	for i, ep := range procDec.ClientVRs {
	// 		valType, ok := procEnv.TypeDefs[ep.ImplQN]
	// 		if !ok {
	// 			err := typedef.ErrSymMissingInEnv(ep.ImplQN)
	// 			s.log.Error("checking failed")
	// 			return err
	// 		}
	// 		wantVal, ok := procEnv.TypeExps[valType.ExpVK]
	// 		if !ok {
	// 			err := typedef.ErrMissingInEnv(valType.ExpVK)
	// 			s.log.Error("checking failed")
	// 			return err
	// 		}
	// 		gotVal, ok := procCtx.Assets[expSpec.Ys[i]]
	// 		if !ok {
	// 			err := procdef.ErrMissingInCtx(ep.ChnlPH)
	// 			s.log.Error("checking failed")
	// 			return err
	// 		}
	// 		err := typeexp.CheckRec(gotVal, wantVal)
	// 		if err != nil {
	// 			s.log.Error("checking failed", slog.Any("want", wantVal), slog.Any("got", gotVal))
	// 			return err
	// 		}
	// 		delete(procCtx.Assets, expSpec.Ys[i])
	// 	}
	// 	// check via
	// 	viaRole, ok := procEnv.TypeDefs[procDec.ProviderVR.ImplQN]
	// 	if !ok {
	// 		err := typedef.ErrSymMissingInEnv(procDec.ProviderVR.ImplQN)
	// 		s.log.Error("checking failed")
	// 		return err
	// 	}
	// 	wantVia, ok := procEnv.TypeExps[viaRole.ExpVK]
	// 	if !ok {
	// 		err := typedef.ErrMissingInEnv(viaRole.ExpVK)
	// 		s.log.Error("checking failed")
	// 		return err
	// 	}
	// 	// check cont
	// 	procCtx.Assets[expSpec.X] = wantVia
	// 	return s.checkType(procEnv, procCtx, procCfg, expSpec.ContES)
	default:
		panic(termexp.ErrExpTypeUnexpected(es))
	}
}

func errOptimisticUpdate(got seqnum.ADT) error {
	return fmt.Errorf("entity concurrent modification: got revision %v", got)
}

func errMissingPool(want uniqsym.ADT) error {
	return fmt.Errorf("pool missing in env: %v", want)
}

func errMissingSig(want identity.ADT) error {
	return fmt.Errorf("sig missing in env: %v", want)
}

func errMissingRole(want uniqsym.ADT) error {
	return fmt.Errorf("role missing in env: %v", want)
}

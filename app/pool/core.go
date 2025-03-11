package pool

import (
	"context"
	"fmt"
	"log/slog"

	"golang.org/x/exp/maps"

	"smecalculus/rolevod/lib/data"
	"smecalculus/rolevod/lib/id"
	"smecalculus/rolevod/lib/ph"
	"smecalculus/rolevod/lib/pol"
	"smecalculus/rolevod/lib/rev"
	"smecalculus/rolevod/lib/sym"

	"smecalculus/rolevod/internal/chnl"
	"smecalculus/rolevod/internal/proc"
	"smecalculus/rolevod/internal/state"
	"smecalculus/rolevod/internal/step"

	"smecalculus/rolevod/app/role"
	"smecalculus/rolevod/app/sig"
)

type ID = id.ADT
type Rev = rev.ADT
type Title = string

type Spec struct {
	Title  string
	SupID  id.ADT
	DepIDs []sig.ID
}

type Ref struct {
	PoolID id.ADT
	Title  string
}

type Lock struct {
	PoolID id.ADT
	Kind   rev.Kind
	Rev    rev.ADT
}

type Root struct {
	PoolID id.ADT
	Title  string
	SupID  id.ADT
	Revs   []rev.ADT
}

const (
	rootRev = rev.Kind(iota)
	procRev
)

type Mod struct {
	Locks []Lock
}

type RootMod struct {
	PoolID id.ADT
	Rev    rev.ADT
	K      rev.Kind
}

type SubSnap struct {
	PoolID id.ADT
	Title  string
	Subs   []Ref
}

type AssetSnap struct {
	PoolID id.ADT
	Title  string
}

type AssetMod struct {
	OutPoolID id.ADT
	InPoolID  id.ADT
	Rev       rev.ADT
	EPs       []proc.Chnl
}

type LiabSnap struct {
	PoolID id.ADT
	Title  string
	EP     proc.Chnl
}

type LiabMod struct {
	OutPoolID id.ADT
	InPoolID  id.ADT
	Rev       rev.ADT
	EP        proc.Chnl
}

type TranSpec struct {
	PoolID id.ADT
	ProcID id.ADT
	Term   step.Term
}

type Environment struct {
	Sigs   map[sig.ID]sig.Root
	Roles  map[role.QN]role.Root
	States map[state.ID]state.Root
	Locks  map[sym.ADT]proc.Lock
}

func (e Environment) Contains(id sig.ID) bool {
	_, ok := e.Sigs[id]
	return ok
}

func (e Environment) LookupPE(id sig.ID) state.EP {
	decl := e.Sigs[id]
	role := e.Roles[decl.X2.Link]
	return state.EP{Z: decl.X2.Link, C: e.States[role.StateID]}
}

func (e Environment) LookupCEs(id sig.ID) []state.EP {
	decl := e.Sigs[id]
	ces := []state.EP{}
	for _, ce := range decl.Ys2 {
		role := e.Roles[decl.X2.Link]
		ces = append(ces, state.EP{Z: ce.Link, C: e.States[role.StateID]})
	}
	return ces
}

// Port
type API interface {
	Create(Spec) (Root, error)
	Retrieve(id.ADT) (SubSnap, error)
	RetreiveRefs() ([]Ref, error)
}

// for compilation purposes
func newAPI() API {
	return &service{}
}

type service struct {
	pools    Repo
	sigs     sig.Repo
	roles    role.Repo
	states   state.Repo
	operator data.Operator
	log      *slog.Logger
}

func newService(
	pools Repo,
	sigs sig.Repo,
	roles role.Repo,
	states state.Repo,
	operator data.Operator,
	l *slog.Logger,
) *service {
	name := slog.String("name", "poolService")
	return &service{pools, sigs, roles, states, operator, l.With(name)}
}

func (s *service) Create(spec Spec) (_ Root, err error) {
	ctx := context.Background()
	s.log.Debug("creation started", slog.Any("spec", spec))
	root := Root{
		PoolID: id.New(),
		Revs:   []rev.ADT{rev.Initial()},
		Title:  spec.Title,
		SupID:  spec.SupID,
	}
	s.operator.Explicit(ctx, func(ds data.Source) error {
		err = s.pools.Insert(ds, root)
		return err
	})
	if err != nil {
		s.log.Error("creation failed")
		return Root{}, err
	}
	s.log.Debug("creation succeeded", slog.Any("id", root.PoolID))
	return root, nil
}

func (s *service) Take(spec TranSpec) (err error) {
	ctx := context.Background()
	// initial values
	poolID := spec.PoolID
	procID := spec.ProcID
	termSpec := spec.Term
	for termSpec != nil {
		var procSnap proc.Snap
		s.operator.Implicit(ctx, func(ds data.Source) {
			procSnap, err = s.pools.SelectProc(ds, procID)
		})
		procAttr := slog.Any("procID", procID)
		if err != nil {
			s.log.Error("taking failed", procAttr)
			return err
		}
		if len(procSnap.Chnls) == 0 {
			s.log.Error("taking failed", procAttr)
			return err
		}
		sigIDs := step.CollectEnv(termSpec)
		var sigs map[sig.ID]sig.Root
		s.operator.Implicit(ctx, func(ds data.Source) {
			sigs, err = s.sigs.SelectEnv(ds, sigIDs)
		})
		if err != nil {
			s.log.Error("taking failed", procAttr, slog.Any("sigs", sigIDs))
			return err
		}
		roleQNs := sig.CollectEnv(maps.Values(sigs))
		var roles map[role.QN]role.Root
		s.operator.Implicit(ctx, func(ds data.Source) {
			roles, err = s.roles.SelectEnv(ds, roleQNs)
		})
		if err != nil {
			s.log.Error("taking failed", procAttr, slog.Any("roles", roleQNs))
			return err
		}
		envIDs := role.CollectEnv(maps.Values(roles))
		ctxIDs := CollectCtx(maps.Values(procSnap.Chnls))
		var states map[state.ID]state.Root
		s.operator.Implicit(ctx, func(ds data.Source) {
			states, err = s.states.SelectEnv(ds, append(envIDs, ctxIDs...))
		})
		if err != nil {
			s.log.Error("taking failed", procAttr, slog.Any("env", envIDs), slog.Any("ctx", ctxIDs))
			return err
		}
		procEnv := Environment{Sigs: sigs, Roles: roles, States: states}
		procCtx := convertToCtx(poolID, maps.Values(procSnap.Chnls), states)
		// type checking
		err = s.checkState(poolID, procEnv, procCtx, procSnap, termSpec)
		if err != nil {
			s.log.Error("taking failed", procAttr)
			return err
		}
		// step taking
		nextSpec, procMod, err := s.takeWith(procCtx, procEnv, procSnap, termSpec)
		if err != nil {
			s.log.Error("taking failed", procAttr)
			return err
		}
		s.operator.Explicit(ctx, func(ds data.Source) error {
			err = s.pools.UpdateProc(ds, procMod)
			if err != nil {
				s.log.Error("taking failed", procAttr)
				return err
			}
			return nil
		})
		if err != nil {
			s.log.Error("taking failed", procAttr)
			return err
		}
		// next values
		poolID = nextSpec.PoolID
		procID = nextSpec.ProcID
		termSpec = nextSpec.Term
	}
	return nil
}

func (s *service) takeWith(
	procCtx state.Context,
	procEnv Environment,
	procCfg proc.Snap,
	ts step.Term,
) (
	tranSpec TranSpec,
	procMod proc.Mod,
	_ error,
) {
	switch termSpec := ts.(type) {
	case step.CloseSpec:
		viaChnl, ok := procCfg.Chnls[termSpec.X]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.X)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		viaAttr := slog.Any("chnlID", viaChnl.ChnlID)
		sndrLock := proc.Lock{
			PoolID: procCfg.PoolID,
			Rev:    procCfg.Rev,
		}
		procMod.Locks = append(procMod.Locks, sndrLock)
		rcvrStep, ok := procCfg.Steps[viaChnl.ChnlID]
		if !ok {
			err := step.ErrMissingInCfg(viaChnl.ChnlID)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		sndrViaBnd := proc.Bnd{
			ChnlPH:  termSpec.X,
			ChnlID:  viaChnl.ChnlID,
			StateID: viaChnl.StateID,
			ProcID:  procCfg.ProcID,
			Rev:     -procCfg.Rev - 1,
		}
		procMod.Bnds = append(procMod.Bnds, sndrViaBnd)
		if rcvrStep == nil {
			sndrStep := step.MsgRoot2{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				ChnlID: viaChnl.ChnlID,
				Rev:    procCfg.Rev + 1,
				Val: step.CloseImpl{
					X: termSpec.X,
				},
			}
			procMod.Steps = append(procMod.Steps, sndrStep)
			s.log.Debug("taking half done", viaAttr)
			return tranSpec, procMod, nil
		}
		svcStep, ok := rcvrStep.(step.SvcRoot2)
		if !ok {
			panic(step.ErrRootTypeUnexpected(rcvrStep))
		}
		switch termImpl := svcStep.Cont.(type) {
		case step.WaitImpl:
			tranSpec = TranSpec{
				PoolID: svcStep.PoolID,
				ProcID: svcStep.ProcID,
				Term:   termImpl.Cont,
			}
			s.log.Debug("taking succeeded", viaAttr)
			return tranSpec, procMod, nil
		default:
			panic(step.ErrContTypeUnexpected2(svcStep.Cont))
		}
	case step.WaitSpec:
		viaChnl, ok := procCfg.Chnls[termSpec.X]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.X)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		viaAttr := slog.Any("chnlID", viaChnl.ChnlID)
		rcvrLock := proc.Lock{
			PoolID: procCfg.PoolID,
			Rev:    procCfg.Rev,
		}
		procMod.Locks = append(procMod.Locks, rcvrLock)
		sndrStep, ok := procCfg.Steps[viaChnl.ChnlID]
		if !ok {
			err := step.ErrMissingInCfg(viaChnl.ChnlID)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		if sndrStep == nil {
			rcvrViaBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  termSpec.X,
				ChnlID:  viaChnl.ChnlID,
				StateID: viaChnl.StateID,
				Rev:     -procCfg.Rev - 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrViaBnd)
			rcvrStep := step.SvcRoot2{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				ChnlID: viaChnl.ChnlID,
				Cont: step.WaitImpl{
					X:    termSpec.X,
					Cont: termSpec.Cont,
				},
			}
			procMod.Steps = append(procMod.Steps, rcvrStep)
			s.log.Debug("taking half done", viaAttr)
			return tranSpec, procMod, nil
		}
		msgStep, ok := sndrStep.(step.MsgRoot2)
		if !ok {
			panic(step.ErrRootTypeUnexpected(sndrStep))
		}
		switch termImpl := msgStep.Val.(type) {
		case step.CloseImpl:
			rcvrViaBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  termSpec.X,
				ChnlID:  viaChnl.ChnlID,
				StateID: viaChnl.StateID,
				Rev:     -procCfg.Rev - 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrViaBnd)
		case step.FwdImpl:
			rcvrViaBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  termSpec.X,
				ChnlID:  termImpl.B,
				StateID: viaChnl.StateID,
				Rev:     procCfg.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrViaBnd)
			tranSpec = TranSpec{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				Term:   termSpec,
			}
			s.log.Debug("taking succeeded", viaAttr)
			return tranSpec, procMod, nil
		default:
			panic(step.ErrValTypeUnexpected2(msgStep.Val))
		}
		tranSpec = TranSpec{
			PoolID: procCfg.PoolID,
			ProcID: procCfg.ProcID,
			Term:   termSpec.Cont,
		}
		s.log.Debug("taking succeeded", viaAttr)
		return tranSpec, procMod, nil
	case step.SendSpec:
		viaChnl, ok := procCfg.Chnls[termSpec.X]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.X)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		viaAttr := slog.Any("chnlID", viaChnl.ChnlID)
		sndrLock := proc.Lock{
			PoolID: procCfg.PoolID,
			Rev:    procCfg.Rev,
		}
		procMod.Locks = append(procMod.Locks, sndrLock)
		rcvrStep, ok := procCfg.Steps[viaChnl.ChnlID]
		if !ok {
			err := step.ErrMissingInCfg(viaChnl.ChnlID)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		valChnl, ok := procCfg.Chnls[termSpec.Y]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.Y)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		sndrValBnd := proc.Bnd{
			ProcID:  procCfg.ProcID,
			ChnlPH:  termSpec.Y,
			ChnlID:  id.Nil,
			StateID: id.Nil,
			Rev:     -procCfg.Rev - 1,
		}
		procMod.Bnds = append(procMod.Bnds, sndrValBnd)
		if rcvrStep == nil {
			sndrStep := step.MsgRoot2{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				ChnlID: viaChnl.ChnlID,
				Rev:    procCfg.Rev,
				Val: step.SendImpl{
					X: termSpec.X,
					A: id.New(),
					B: valChnl.ChnlID,
				},
			}
			procMod.Steps = append(procMod.Steps, sndrStep)
			s.log.Debug("taking half done", viaAttr)
			return tranSpec, procMod, nil
		}
		svcStep, ok := rcvrStep.(step.SvcRoot2)
		if !ok {
			panic(step.ErrRootTypeUnexpected(rcvrStep))
		}
		viaState, ok := procCtx.Linear[termSpec.X]
		if !ok {
			err := state.ErrMissingInCtx(termSpec.X)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		viaStateID := viaState.(state.Prod).Next()
		switch termImpl := svcStep.Cont.(type) {
		case step.RecvImpl:
			rcvrViaBnd := proc.Bnd{
				ProcID:  svcStep.ProcID,
				ChnlPH:  termImpl.X,
				ChnlID:  termImpl.A,
				StateID: viaStateID,
				Rev:     svcStep.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrViaBnd)
			sndrViaBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  termSpec.X,
				ChnlID:  termImpl.A,
				StateID: viaStateID,
				Rev:     procCfg.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, sndrViaBnd)
			rcvrValBnd := proc.Bnd{
				ProcID:  svcStep.ProcID,
				ChnlPH:  termImpl.Y,
				ChnlID:  valChnl.ChnlID,
				StateID: valChnl.StateID,
				Rev:     svcStep.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrValBnd)
			tranSpec = TranSpec{
				PoolID: svcStep.PoolID,
				ProcID: svcStep.ProcID,
				Term:   termImpl.Cont,
			}
			s.log.Debug("taking succeeded", viaAttr)
			return tranSpec, procMod, nil
		default:
			panic(step.ErrContTypeUnexpected2(svcStep.Cont))
		}
	case step.RecvSpec:
		viaChnl, ok := procCfg.Chnls[termSpec.X]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.X)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		viaAttr := slog.Any("chnlID", viaChnl.ChnlID)
		rcvrLock := proc.Lock{
			PoolID: procCfg.PoolID,
			Rev:    procCfg.Rev,
		}
		procMod.Locks = append(procMod.Locks, rcvrLock)
		sndrStep, ok := procCfg.Steps[viaChnl.ChnlID]
		if !ok {
			err := step.ErrMissingInCfg(viaChnl.ChnlID)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		if sndrStep == nil {
			rcvrStep := step.SvcRoot2{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				ChnlID: viaChnl.ChnlID,
				Rev:    procCfg.Rev,
				Cont: step.RecvImpl{
					X:    termSpec.X,
					A:    id.New(),
					Cont: termSpec.Cont,
				},
			}
			procMod.Steps = append(procMod.Steps, rcvrStep)
			s.log.Debug("taking half done", viaAttr)
			return tranSpec, procMod, nil
		}
		msgStep, ok := sndrStep.(step.MsgRoot2)
		if !ok {
			panic(step.ErrRootTypeUnexpected(sndrStep))
		}
		viaState, ok := procCtx.Linear[termSpec.X]
		if !ok {
			err := state.ErrMissingInCtx(termSpec.X)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		viaStateID := viaState.(state.Prod).Next()
		valState, ok := procCtx.Linear[termSpec.Y]
		if !ok {
			err := state.ErrMissingInCtx(termSpec.Y)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		switch termImpl := msgStep.Val.(type) {
		case step.SendImpl:
			sndrViaBnd := proc.Bnd{
				ProcID:  msgStep.ProcID,
				ChnlPH:  termImpl.X,
				ChnlID:  termImpl.A,
				StateID: viaStateID,
				Rev:     msgStep.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, sndrViaBnd)
			rcvrViaBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  termSpec.X,
				ChnlID:  termImpl.A,
				StateID: viaStateID,
				Rev:     procCfg.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrViaBnd)
			rcvrValBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  termSpec.Y,
				ChnlID:  termImpl.B,
				StateID: valState.Ident(),
				Rev:     procCfg.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrValBnd)
			tranSpec = TranSpec{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				Term:   termSpec.Cont,
			}
			s.log.Debug("taking succeeded", viaAttr)
			return tranSpec, procMod, nil
		default:
			panic(step.ErrValTypeUnexpected2(msgStep.Val))
		}
	case step.LabSpec:
		viaChnl, ok := procCfg.Chnls[termSpec.X]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.X)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		viaAttr := slog.Any("chnlID", viaChnl.ChnlID)
		sndrLock := proc.Lock{
			PoolID: procCfg.PoolID,
			Rev:    procCfg.Rev,
		}
		procMod.Locks = append(procMod.Locks, sndrLock)
		rcvrStep, ok := procCfg.Steps[viaChnl.ChnlID]
		if !ok {
			err := step.ErrMissingInCfg(viaChnl.ChnlID)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		viaState, ok := procCtx.Linear[termSpec.X]
		if !ok {
			err := state.ErrMissingInCtx(termSpec.X)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		viaStateID := viaState.(state.Sum).Next(termSpec.L)
		if rcvrStep == nil {
			sndrViaBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  termSpec.X,
				ChnlID:  id.New(),
				StateID: viaStateID,
				Rev:     procCfg.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, sndrViaBnd)
			sndrStep := step.MsgRoot2{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				ChnlID: viaChnl.ChnlID,
				Rev:    procCfg.Rev + 1,
				Val: step.LabImpl{
					X: termSpec.X,
					A: sndrViaBnd.ChnlID,
					L: termSpec.L,
				},
			}
			procMod.Steps = append(procMod.Steps, sndrStep)
			s.log.Debug("taking half done", viaAttr)
			return tranSpec, procMod, nil
		}
		svcStep, ok := rcvrStep.(step.SvcRoot2)
		if !ok {
			panic(step.ErrRootTypeUnexpected(rcvrStep))
		}
		switch termImpl := svcStep.Cont.(type) {
		case step.CaseImpl:
			rcvrViaBnd := proc.Bnd{
				ProcID:  svcStep.ProcID,
				ChnlPH:  termImpl.X,
				ChnlID:  termImpl.A,
				StateID: viaStateID,
				Rev:     svcStep.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrViaBnd)
			tranSpec = TranSpec{
				PoolID: svcStep.PoolID,
				ProcID: svcStep.ProcID,
				Term:   termImpl.Conts[termSpec.L],
			}
			s.log.Debug("taking succeeded", viaAttr)
			return tranSpec, procMod, nil
		default:
			panic(step.ErrContTypeUnexpected2(svcStep.Cont))
		}
	case step.CaseSpec:
		viaChnl, ok := procCfg.Chnls[termSpec.X]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.X)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		viaAttr := slog.Any("chnlID", viaChnl.ChnlID)
		rcvrLock := proc.Lock{
			PoolID: procCfg.PoolID,
			Rev:    procCfg.Rev,
		}
		procMod.Locks = append(procMod.Locks, rcvrLock)
		sndrStep, ok := procCfg.Steps[viaChnl.ChnlID]
		if !ok {
			err := step.ErrMissingInCfg(viaChnl.ChnlID)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		if sndrStep == nil {
			rcvrStep := step.SvcRoot2{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				ChnlID: viaChnl.ChnlID,
				Rev:    procCfg.Rev + 1,
				Cont: step.CaseImpl{
					X:     termSpec.X,
					A:     id.New(),
					Conts: termSpec.Conts,
				},
			}
			procMod.Steps = append(procMod.Steps, rcvrStep)
			s.log.Debug("taking half done", viaAttr)
			return tranSpec, procMod, nil
		}
		msgStep, ok := sndrStep.(step.MsgRoot2)
		if !ok {
			panic(step.ErrRootTypeUnexpected(sndrStep))
		}
		switch termImpl := msgStep.Val.(type) {
		case step.LabImpl:
			viaState, ok := procCtx.Linear[termSpec.X]
			if !ok {
				err := state.ErrMissingInCtx(termSpec.X)
				s.log.Error("taking failed", viaAttr)
				return TranSpec{}, proc.Mod{}, err
			}
			viaStateID := viaState.(state.Sum).Next(termImpl.L)
			// sndrViaBnd := proc.Bnd{
			// 	ProcID:  msgStep.ProcID,
			// 	ChnlPH:  termImpl.X,
			// 	ChnlID:  termImpl.A,
			// 	StateID: viaStateID,
			// 	Rev:     msgStep.Rev + 1,
			// }
			// procMod.Bnds = append(procMod.Bnds, sndrViaBnd)
			rcvrViaBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  termSpec.X,
				ChnlID:  termImpl.A,
				StateID: viaStateID,
				Rev:     procCfg.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrViaBnd)
			tranSpec = TranSpec{
				PoolID: procCfg.PoolID,
				ProcID: procCfg.ProcID,
				Term:   termSpec.Conts[termImpl.L],
			}
			s.log.Debug("taking succeeded", viaAttr)
			return tranSpec, procMod, nil
		default:
			panic(step.ErrValTypeUnexpected2(msgStep.Val))
		}
	case step.SpawnSpec:
		rcvrSnap, ok := procEnv.Locks[termSpec.PoolQN]
		if !ok {
			err := errMissingPool(termSpec.PoolQN)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		rcvrLiab := proc.Liab{
			ProcID: id.New(),
			PoolID: rcvrSnap.PoolID,
			Rev:    rcvrSnap.Rev + 1,
		}
		procMod.Liabs = append(procMod.Liabs, rcvrLiab)
		rcvrSig, ok := procEnv.Sigs[termSpec.SigID]
		if !ok {
			err := errMissingSig(termSpec.SigID)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		rcvrRole, ok := procEnv.Roles[rcvrSig.X.RoleQN]
		if !ok {
			err := errMissingRole(rcvrSig.X.RoleQN)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		sndrViaBnd := proc.Bnd{
			ProcID:  procCfg.ProcID,
			ChnlPH:  termSpec.X,
			ChnlID:  id.New(),
			StateID: rcvrRole.StateID,
			Rev:     procCfg.Rev + 1,
		}
		procMod.Bnds = append(procMod.Bnds, sndrViaBnd)
		rcvrViaBnd := proc.Bnd{
			ProcID:  rcvrLiab.ProcID,
			ChnlPH:  rcvrSig.X.ChnlPH,
			ChnlID:  sndrViaBnd.ChnlID,
			StateID: rcvrRole.StateID,
			Rev:     rcvrSnap.Rev + 1,
		}
		procMod.Bnds = append(procMod.Bnds, rcvrViaBnd)
		for i, chnlPH := range termSpec.Ys {
			valChnl, ok := procCfg.Chnls[chnlPH]
			if !ok {
				err := proc.ErrMissingChnl(chnlPH)
				s.log.Error("taking failed")
				return TranSpec{}, proc.Mod{}, err
			}
			sndrValBnd := proc.Bnd{
				ProcID:  procCfg.ProcID,
				ChnlPH:  chnlPH,
				ChnlID:  valChnl.ChnlID,
				StateID: valChnl.StateID,
				Rev:     -procCfg.Rev - 1,
			}
			procMod.Bnds = append(procMod.Bnds, sndrValBnd)
			rcvrValBnd := proc.Bnd{
				ProcID:  rcvrLiab.ProcID,
				ChnlPH:  rcvrSig.Ys[i].ChnlPH,
				ChnlID:  valChnl.ChnlID,
				StateID: valChnl.StateID,
				Rev:     rcvrSnap.Rev + 1,
			}
			procMod.Bnds = append(procMod.Bnds, rcvrValBnd)
		}
		tranSpec = TranSpec{
			PoolID: procCfg.PoolID,
			ProcID: procCfg.ProcID,
			Term:   termSpec.Cont,
		}
		s.log.Debug("taking succeeded")
		return tranSpec, procMod, nil
	case step.FwdSpec:
		viaChnl, ok := procCfg.Chnls[termSpec.X]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.X)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		viaAttr := slog.Any("chnlID", viaChnl.ChnlID)
		viaState, ok := procCtx.Linear[termSpec.X]
		if !ok {
			err := state.ErrMissingInCtx(termSpec.X)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		viaStep, ok := procCfg.Steps[viaChnl.ChnlID]
		if !ok {
			err := step.ErrMissingInCfg(viaChnl.ChnlID)
			s.log.Error("taking failed", viaAttr)
			return TranSpec{}, proc.Mod{}, err
		}
		valChnl, ok := procCfg.Chnls[termSpec.Y]
		if !ok {
			err := chnl.ErrMissingInCfg(termSpec.Y)
			s.log.Error("taking failed")
			return TranSpec{}, proc.Mod{}, err
		}
		switch viaState.Pol() {
		case pol.Pos:
			switch vs := viaStep.(type) {
			case step.SvcRoot2:
				xBnd := proc.Bnd{
					ProcID:  vs.ProcID,
					ChnlPH:  vs.Cont.Via(),
					ChnlID:  viaChnl.ChnlID,
					StateID: viaChnl.StateID,
					Rev:     vs.Rev + 1,
				}
				procMod.Bnds = append(procMod.Bnds, xBnd)
				tranSpec = TranSpec{
					PoolID: vs.PoolID,
					ProcID: vs.ProcID,
					Term:   vs.Cont,
				}
				s.log.Debug("taking succeeded", viaAttr)
				return tranSpec, procMod, nil
			case step.MsgRoot2:
				yBnd := proc.Bnd{
					ProcID:  vs.ProcID,
					ChnlPH:  vs.Val.Via(),
					ChnlID:  valChnl.ChnlID,
					StateID: valChnl.StateID,
					Rev:     vs.Rev + 1,
				}
				procMod.Bnds = append(procMod.Bnds, yBnd)
				tranSpec = TranSpec{
					PoolID: vs.PoolID,
					ProcID: vs.ProcID,
					Term:   vs.Val,
				}
				s.log.Debug("taking succeeded", viaAttr)
				return tranSpec, procMod, nil
			case nil:
				xBnd := proc.Bnd{
					ProcID:  procCfg.ProcID,
					ChnlPH:  termSpec.X,
					ChnlID:  viaChnl.ChnlID,
					StateID: id.Nil,
					Rev:     -procCfg.Rev - 1,
				}
				procMod.Bnds = append(procMod.Bnds, xBnd)
				yBnd := proc.Bnd{
					ProcID:  procCfg.ProcID,
					ChnlPH:  termSpec.Y,
					ChnlID:  valChnl.ChnlID,
					StateID: id.Nil,
					Rev:     -procCfg.Rev - 1,
				}
				procMod.Bnds = append(procMod.Bnds, yBnd)
				msgStep := step.MsgRoot2{
					PoolID: procCfg.PoolID,
					ProcID: procCfg.ProcID,
					ChnlID: viaChnl.ChnlID,
					Rev:    procCfg.Rev,
					Val: step.FwdImpl{
						B: valChnl.ChnlID,
					},
				}
				procMod.Steps = append(procMod.Steps, msgStep)
				s.log.Debug("taking half done", viaAttr)
				return tranSpec, procMod, nil
			default:
				panic(step.ErrRootTypeUnexpected(viaStep))
			}
		case pol.Neg:
			switch vs := viaStep.(type) {
			case step.SvcRoot2:
				yBnd := proc.Bnd{
					ProcID:  vs.ProcID,
					ChnlPH:  vs.Cont.Via(),
					ChnlID:  valChnl.ChnlID,
					StateID: valChnl.StateID,
					Rev:     vs.Rev + 1,
				}
				procMod.Bnds = append(procMod.Bnds, yBnd)
				tranSpec = TranSpec{
					PoolID: vs.PoolID,
					ProcID: vs.ProcID,
					Term:   vs.Cont,
				}
				s.log.Debug("taking succeeded", viaAttr)
				return tranSpec, procMod, nil
			case step.MsgRoot2:
				xBnd := proc.Bnd{
					ProcID:  vs.ProcID,
					ChnlPH:  vs.Val.Via(),
					ChnlID:  viaChnl.ChnlID,
					StateID: viaChnl.StateID,
					Rev:     vs.Rev + 1,
				}
				procMod.Bnds = append(procMod.Bnds, xBnd)
				tranSpec = TranSpec{
					PoolID: vs.PoolID,
					ProcID: vs.ProcID,
					Term:   vs.Val,
				}
				s.log.Debug("taking succeeded", viaAttr)
				return tranSpec, procMod, nil
			case nil:
				svcStep := step.SvcRoot2{
					PoolID: procCfg.PoolID,
					ProcID: procCfg.ProcID,
					ChnlID: viaChnl.ChnlID,
					Rev:    procCfg.Rev,
					Cont: step.FwdImpl{
						B: valChnl.ChnlID,
					},
				}
				procMod.Steps = append(procMod.Steps, svcStep)
				s.log.Debug("taking half done", viaAttr)
				return tranSpec, procMod, nil
			default:
				panic(step.ErrRootTypeUnexpected(viaStep))
			}
		default:
			panic(state.ErrPolarityUnexpected(viaState))
		}
	default:
		panic(step.ErrTermTypeUnexpected(ts))
	}
}

func (s *service) Retrieve(poolID id.ADT) (snap SubSnap, err error) {
	ctx := context.Background()
	s.operator.Implicit(ctx, func(ds data.Source) {
		snap, err = s.pools.SelectSubs(ds, poolID)
	})
	if err != nil {
		s.log.Error("retrieval failed", slog.Any("id", poolID))
		return SubSnap{}, err
	}
	return snap, nil
}

func (s *service) RetreiveRefs() (refs []Ref, err error) {
	ctx := context.Background()
	s.operator.Implicit(ctx, func(ds data.Source) {
		refs, err = s.pools.SelectRefs(ds)
	})
	if err != nil {
		s.log.Error("retrieval failed")
		return nil, err
	}
	return refs, nil
}

func CollectCtx(chnls []proc.Chnl) []state.ID {
	return nil
}

func convertToCtx(poolID id.ADT, chnls []proc.Chnl, states map[state.ID]state.Root) state.Context {
	linear := make(map[ph.ADT]state.Root, len(chnls))
	for _, ch := range chnls {
		if poolID != ch.PoolID {
			linear[ch.ChnlPH] = states[ch.StateID]
		}
	}
	return state.Context{Linear: linear}
}

func (s *service) checkState(
	poolID id.ADT,
	procEnv Environment,
	procCtx state.Context,
	procSnap proc.Snap,
	termSpec step.Term,
) error {
	ch, ok := procSnap.Chnls[termSpec.Via()]
	if !ok {
		panic("no via in proc snap")
	}
	if poolID == ch.PoolID {
		return s.checkProvider(poolID, procEnv, procCtx, procSnap, termSpec)
	} else {
		return s.checkClient(poolID, procEnv, procCtx, procSnap, termSpec)
	}
}

func (s *service) checkProvider(
	poolID id.ADT,
	procEnv Environment,
	procCtx state.Context,
	procSnap proc.Snap,
	termSpec step.Term,
) error {
	return nil
}

func (s *service) checkClient(
	poolID id.ADT,
	procEnv Environment,
	procCtx state.Context,
	procSnap proc.Snap,
	termSpec step.Term,
) error {
	return nil
}

// Port
type Repo interface {
	Insert(data.Source, Root) error
	SelectRefs(data.Source) ([]Ref, error)
	SelectSubs(data.Source, id.ADT) (SubSnap, error)
	SelectAssets(data.Source, id.ADT) (AssetSnap, error)
	SelectProc(data.Source, id.ADT) (proc.Snap, error)
	UpdateProc(data.Source, proc.Mod) error
	UpdateAssets(data.Source, AssetMod) error
	Transfer(source data.Source, giver id.ADT, taker id.ADT, pids []chnl.ID) error
}

// goverter:variables
// goverter:output:format assign-variable
// goverter:extend smecalculus/rolevod/lib/id:Convert.*
var (
	ConvertRootToRef func(Root) Ref
)

func errOptimisticUpdate(got rev.ADT) error {
	return fmt.Errorf("entity concurrent modification: got revision %v", got)
}

func errMissingPool(want sym.ADT) error {
	return fmt.Errorf("pool missing in env: %v", want)
}

func errMissingSig(want id.ADT) error {
	return fmt.Errorf("sig missing in env: %v", want)
}

func errMissingRole(want sym.ADT) error {
	return fmt.Errorf("role missing in env: %v", want)
}

package deal

import (
	"context"
	"fmt"
	"log/slog"

	"golang.org/x/exp/maps"

	"smecalculus/rolevod/lib/ak"
	"smecalculus/rolevod/lib/data"
	"smecalculus/rolevod/lib/id"
	"smecalculus/rolevod/lib/ph"
	"smecalculus/rolevod/lib/pol"
	"smecalculus/rolevod/lib/sym"

	"smecalculus/rolevod/internal/chnl"
	"smecalculus/rolevod/internal/state"
	"smecalculus/rolevod/internal/step"

	"smecalculus/rolevod/app/pool"
	"smecalculus/rolevod/app/role"
	"smecalculus/rolevod/app/sig"
)

type Name = string
type ID = id.ADT

type Spec struct {
	Name Name
}

type Ref struct {
	ID   ID
	Name Name
}

// aka Configuration or Eta
type Root struct {
	ID       ID
	Name     Name
	Children []Ref
	Sigs     []sig.Ref
}

type Environment struct {
	sigs   map[sig.ID]sig.Root
	roles  map[role.QN]role.Root
	states map[state.ID]state.Root
}

func (e Environment) Contains(id sig.ID) bool {
	_, ok := e.sigs[id]
	return ok
}

func (e Environment) LookupPE(id sig.ID) state.EP {
	decl := e.sigs[id]
	role := e.roles[decl.X2.Link]
	return state.EP{Z: decl.X2.Link, C: e.states[role.StateID]}
}

func (e Environment) LookupCEs(id sig.ID) []state.EP {
	decl := e.sigs[id]
	ces := []state.EP{}
	for _, ce := range decl.Ys2 {
		role := e.roles[decl.X2.Link]
		ces = append(ces, state.EP{Z: ce.Link, C: e.states[role.StateID]})
	}
	return ces
}

type Configuration struct {
	chnls  map[chnl.ID]chnl.Root
	states map[state.ID]state.Root
}

func (c *Configuration) LookupCh(id chnl.ID) (chnl.Root, bool) {
	ch, ok := c.chnls[id]
	if !ok {
		return chnl.Root{}, false
	}
	return ch, true
}

func (c *Configuration) LookupSt(id chnl.ID) (state.Root, bool) {
	ch, ok := c.chnls[id]
	if !ok {
		return nil, false
	}
	if ch.StateID == nil {
		return nil, false
	}
	st, ok := c.states[*ch.StateID]
	if !ok {
		panic(state.ErrMissingInCfg(*ch.StateID))
	}
	return st, true
}

func (c *Configuration) Add(ch chnl.Root) {
	c.chnls[ch.ID] = ch
}

func (c *Configuration) Remove(ids ...chnl.ID) {
	for _, id := range ids {
		delete(c.chnls, id)
	}
}

// goverter:variables
// goverter:output:format assign-variable
// goverter:extend smecalculus/rolevod/lib/id:Convert.*
var (
	ConvertRootToRef func(Root) Ref
)

type API interface {
	Create(Spec) (Root, error)
	Retrieve(ID) (Root, error)
	RetreiveAll() ([]Ref, error)
	Involve(PartSpec) (chnl.Root, error)
	Take(TranSpec) error
}

type service struct {
	deals    repo
	roles    role.Repo
	sigs     sig.Repo
	chnls    chnl.Repo
	pools    pool.Repo
	steps    step.Repo
	states   state.Repo
	operator data.Operator
	log      *slog.Logger
}

// for compilation purposes
func newAPI() API {
	return &service{}
}

func newService(
	deals repo,
	roles role.Repo,
	sigs sig.Repo,
	chnls chnl.Repo,
	pools pool.Repo,
	steps step.Repo,
	states state.Repo,
	operator data.Operator,
	l *slog.Logger,
) *service {
	name := slog.String("name", "dealService")
	return &service{
		deals, roles, sigs, chnls, pools, steps, states, operator, l.With(name),
	}
}

func (s *service) Create(spec Spec) (Root, error) {
	s.log.Debug("deal creation started", slog.Any("spec", spec))
	root := Root{
		ID:   id.New(),
		Name: spec.Name,
	}
	err := s.deals.Insert(root)
	if err != nil {
		s.log.Error("deal insertion failed",
			slog.Any("reason", err),
			slog.Any("root", root),
		)
		return root, err
	}
	s.log.Debug("deal creation succeeded", slog.Any("root", root))
	return root, nil
}

func (s *service) Retrieve(id ID) (Root, error) {
	root, err := s.deals.SelectByID(id)
	if err != nil {
		return Root{}, err
	}
	return root, nil
}

func (s *service) RetreiveAll() ([]Ref, error) {
	return s.deals.SelectAll()
}

func (s *service) Involve(gotSpec PartSpec) (_ chnl.Root, err error) {
	ctx := context.Background()
	s.log.Debug("involvement started", slog.Any("spec", gotSpec))
	var wantSig sig.Root
	s.operator.Implicit(ctx, func(ds data.Source) {
		wantSig, err = s.sigs.SelectByID(ds, gotSpec.Service)
	})
	if err != nil {
		s.log.Error("sig selection failed", slog.Any("id", gotSpec.Service))
		return chnl.Root{}, err
	}
	var wantRole role.Root
	s.operator.Implicit(ctx, func(ds data.Source) {
		wantRole, err = s.roles.SelectByFQN(ds, wantSig.X2.Link)
	})
	if err != nil {
		s.log.Error("role selection failed", slog.Any("fqn", wantSig.X2.Link))
		return chnl.Root{}, err
	}
	newPE := chnl.Root{
		ID:      id.New(),
		Title:   wantSig.X2.Key,
		StateID: &wantRole.StateID,
		PoolID:  &gotSpec.Servant,
	}
	newProc := step.ProcRoot{
		ID:  id.New(),
		PID: newPE.ID,
		Term: step.CTASpec{
			AK:  ak.New(),
			Sig: gotSpec.Service,
		},
	}
	s.operator.Explicit(ctx, func(ds data.Source) error {
		err = s.chnls.Insert(ds, newPE)
		if err != nil {
			s.log.Error("providable endpoint insertion failed", slog.Any("pe", newPE))
			return err
		}
		if len(gotSpec.Resources) > 0 {
			err = s.pools.Transfer(ds, gotSpec.Steward, gotSpec.Servant, gotSpec.Resources)
			if err != nil {
				s.log.Error("context transfer failed",
					slog.Any("producer", gotSpec.Steward),
					slog.Any("consumer", gotSpec.Servant),
					slog.Any("resources", gotSpec.Resources),
				)
				return err
			}
		}
		err = s.steps.Insert(ds, newProc)
		if err != nil {
			s.log.Error("process insertion failed", slog.Any("proc", newProc))
			return err
		}
		return nil
	})
	if err != nil {
		s.log.Error("involvement failed")
		return chnl.Root{}, err
	}
	s.log.Debug("involvement succeeded", slog.Any("proc", newProc))
	return newPE, nil
}

func (s *service) Take(spec TranSpec) (err error) {
	ctx := context.Background()
	if spec.Term == nil {
		panic(step.ErrTermValueNil(spec.ProcID))
	}
	s.log.Debug("taking started", slog.Any("spec", spec))
	// proc checking
	var curStep step.Root
	s.operator.Implicit(ctx, func(ds data.Source) {
		curStep, err = s.steps.SelectByPID(ds, spec.ProcID)
	})
	pidAttr := slog.Any("pid", spec.ProcID)
	if err != nil {
		s.log.Error("taking failed", pidAttr)
		return err
	}
	if curStep == nil {
		err = step.ErrDoesNotExist(spec.ProcID)
		s.log.Error("taking failed", pidAttr)
		return err
	}
	proc, ok := curStep.(step.ProcRoot)
	if !ok {
		err = step.ErrRootTypeMismatch(curStep, step.ProcRoot{})
		s.log.Error("taking failed", pidAttr)
		return err
	}
	_, ok = proc.Term.(step.CTASpec)
	if !ok {
		err = step.ErrTermTypeMismatch(spec.Term, step.CTASpec{})
		s.log.Error("taking failed", pidAttr)
		return err
	}
	sigIDs := step.CollectEnv(spec.Term)
	var sigs map[sig.ID]sig.Root
	s.operator.Implicit(ctx, func(ds data.Source) {
		sigs, err = s.sigs.SelectEnv(ds, sigIDs)
	})
	if err != nil {
		s.log.Error("taking failed", pidAttr, slog.Any("sigs", sigIDs))
		return err
	}
	roleFQNs := sig.CollectEnv(maps.Values(sigs))
	var roles map[sym.ADT]role.Root
	s.operator.Implicit(ctx, func(ds data.Source) {
		roles, err = s.roles.SelectEnv(ds, roleFQNs)
	})
	if err != nil {
		s.log.Error("taking failed", pidAttr, slog.Any("roles", roleFQNs))
		return err
	}
	var pe chnl.Root
	s.operator.Implicit(ctx, func(ds data.Source) {
		pe, err = s.chnls.SelectByID(ds, proc.PID)
	})
	if err != nil {
		s.log.Error("taking failed", pidAttr)
		return err
	}
	ceIDs := step.CollectCtx(proc.PID, spec.Term)
	var ces []chnl.Root
	s.operator.Implicit(ctx, func(ds data.Source) {
		ces, err = s.chnls.SelectCtx(proc.PID, ceIDs)
	})
	if err != nil {
		s.log.Error("taking failed", pidAttr, slog.Any("ids", ceIDs))
		return err
	}
	envIDs := role.CollectEnv(maps.Values(roles))
	ctxIDs := chnl.CollectCtx(append(ces, pe))
	var states map[state.ID]state.Root
	s.operator.Implicit(ctx, func(ds data.Source) {
		states, err = s.states.SelectEnv(ds, append(envIDs, ctxIDs...))
	})
	if err != nil {
		s.log.Error("taking failed", pidAttr, slog.Any("env", envIDs), slog.Any("ctx", ctxIDs))
		return err
	}
	env := Environment{sigs, roles, states}
	ctx2 := convertToCtx(ces, states)
	zc := state.EP{Z: pe.ID, C: states[*pe.StateID]}
	// type checking
	err = s.checkState(env, ctx2, zc, spec.Term)
	if err != nil {
		s.log.Error("taking failed", pidAttr)
		return err
	}
	// step taking
	cfg := Configuration{chnls: convertToCfg(append(ces, pe)), states: states}
	proc.Term = spec.Term
	return s.takeProcWith(proc, cfg)
}

func (s *service) takeProc(
	proc step.ProcRoot,
) (err error) {
	s.log.Debug("transition taking started", slog.Any("proc", proc))
	pe, err := s.chnls.SelectByID(proc.PID)
	if err != nil {
		s.log.Error("providable endpoint selection failed",
			slog.Any("reason", err),
			slog.Any("id", proc.PID),
		)
		return err
	}
	ceIDs := step.CollectCtx(proc.PID, proc.Term)
	ces, err := s.chnls.SelectCtx(proc.PID, ceIDs)
	if err != nil {
		s.log.Error("consumable endpoints selection failed",
			slog.Any("reason", err),
			slog.Any("ids", ceIDs),
		)
		return err
	}
	stIDs := chnl.CollectCtx(append(ces, pe))
	states, err := s.states.SelectEnv(stIDs)
	if err != nil {
		s.log.Error("states selection failed",
			slog.Any("reason", err),
			slog.Any("ids", stIDs),
		)
		return err
	}
	cfg := Configuration{chnls: convertToCfg(append(ces, pe)), states: states}
	return s.takeProcWith(proc, cfg)
}

func (s *service) takeProcWith(
	proc step.ProcRoot,
	cfg Configuration,
) (err error) {
	switch term := proc.Term.(type) {
	case step.CloseSpec:
		viaID, ok := term.X.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(term.X)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curVia, ok := cfg.LookupCh(viaID)
		if !ok {
			err := chnl.ErrMissingInCfg(viaID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curSem, err := s.steps.SelectByVID(viaID)
		if err != nil {
			s.log.Error("service selection failed",
				slog.Any("reason", err),
				slog.Any("vid", curVia.ID),
			)
			return err
		}
		if curSem == nil {
			newMsg := step.MsgRoot{
				ID:  id.New(),
				PID: proc.PID,
				VID: curVia.ID,
				Val: term,
			}
			err := s.steps.Insert(newMsg)
			if err != nil {
				s.log.Error("message insertion failed",
					slog.Any("reason", err),
					slog.Any("msg", newMsg),
				)
				return err
			}
			s.log.Debug("transition taking half done", slog.Any("msg", newMsg))
			return nil
		}
		srv, ok := curSem.(step.SrvRoot)
		if !ok {
			err = step.ErrRootTypeUnexpected(curSem)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		wait, ok := srv.Cont.(step.WaitSpec)
		if !ok {
			err = fmt.Errorf("cont type mismatch: want %T, got %T", wait, srv.Cont)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				slog.Any("cont", srv.Cont),
			)
			return err
		}
		// consume and close channel
		finVia := chnl.Root{
			ID:      id.New(),
			Key:     curVia.Key,
			PreID:   &curVia.ID,
			StateID: nil,
		}
		err = s.chnls.Insert(finVia)
		if err != nil {
			s.log.Error("channel insertion failed",
				slog.Any("reason", err),
				slog.Any("via", finVia),
			)
			return err
		}
		newProc := step.ProcRoot{
			ID:   id.New(),
			PID:  srv.PID,
			Term: wait.Cont,
		}
		s.log.Debug("transition taking succeeded")
		return s.takeProc(newProc)
	case step.WaitSpec:
		viaID, ok := term.X.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(term.X)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curVia, ok := cfg.LookupCh(viaID)
		if !ok {
			err = chnl.ErrMissingInCfg(viaID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curSem, err := s.steps.SelectByVID(curVia.ID)
		if err != nil {
			s.log.Error("message selection failed",
				slog.Any("reason", err),
				slog.Any("vid", curVia.ID),
			)
			return err
		}
		if curSem == nil {
			newSrv := step.SrvRoot{
				ID:   id.New(),
				PID:  proc.PID,
				VID:  curVia.ID,
				Cont: term,
			}
			err = s.steps.Insert(newSrv)
			if err != nil {
				s.log.Error("service insertion failed",
					slog.Any("reason", err),
					slog.Any("srv", newSrv),
				)
				return err
			}
			s.log.Debug("transition taking half done", slog.Any("srv", newSrv))
			return nil
		}
		msg, ok := curSem.(step.MsgRoot)
		if !ok {
			err = step.ErrRootTypeMismatch(curSem, step.MsgRoot{})
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				// TODO log some ID
			)
			return err
		}
		var newProc step.ProcRoot
		switch val := msg.Val.(type) {
		case step.CloseSpec:
			// consume and close channel
			finVia := chnl.Root{
				ID:      id.New(),
				Key:     curVia.Key,
				PreID:   &curVia.ID,
				StateID: nil,
			}
			err = s.chnls.Insert(finVia)
			if err != nil {
				s.log.Error("channel insertion failed",
					slog.Any("reason", err),
					slog.Any("via", finVia),
				)
				return err
			}
			newProc = step.ProcRoot{
				ID:   id.New(),
				PID:  proc.PID,
				Term: term.Cont,
			}
		case step.FwdSpec:
			d, ok := val.Y.(chnl.ID)
			if !ok {
				err = chnl.ErrNotAnID(val.Y)
				s.log.Error("transition taking failed",
					slog.Any("reason", err),
				)
				return err
			}
			err := s.chnls.Transfer(msg.PID, proc.PID, []chnl.ID{d})
			if err != nil {
				s.log.Error("channel transfer failed",
					slog.Any("reason", err),
					slog.Any("from", msg.PID),
					slog.Any("to", proc.PID),
					slog.Any("id", d),
				)
				return err
			}
			newProc = step.ProcRoot{
				ID:   id.New(),
				PID:  proc.PID,
				Term: step.Subst(term, val.X, d),
			}
		default:
			panic(step.ErrValTypeUnexpected(msg.Val))
		}
		s.log.Debug("transition taking succeeded")
		return s.takeProc(newProc)
	case step.SendSpec:
		viaID, ok := term.X.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(term.X)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curVia, ok := cfg.LookupCh(viaID)
		if !ok {
			err = chnl.ErrMissingInCfg(viaID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curSem, err := s.steps.SelectByVID(curVia.ID)
		if err != nil {
			s.log.Error("service selection failed",
				slog.Any("reason", err),
				slog.Any("vid", curVia.ID),
			)
			return err
		}
		if curSem == nil {
			newMsg := step.MsgRoot{
				ID:  id.New(),
				PID: proc.PID,
				VID: curVia.ID,
				Val: term,
			}
			err = s.steps.Insert(newMsg)
			if err != nil {
				s.log.Error("message insertion failed",
					slog.Any("reason", err),
					slog.Any("msg", newMsg),
				)
				return err
			}
			s.log.Debug("transition taking half done", slog.Any("msg", newMsg))
			return nil
		}
		srv, ok := curSem.(step.SrvRoot)
		if !ok {
			err = step.ErrRootTypeMismatch(curSem, step.SrvRoot{})
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				// TODO log some ID
			)
			return err
		}
		recv, ok := srv.Cont.(step.RecvSpec)
		if !ok {
			err = fmt.Errorf("cont type mismatch: want %T, got %T", recv, srv.Cont)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				slog.Any("cont", srv.Cont),
			)
			return err
		}
		curSt, ok := cfg.LookupSt(curVia.ID)
		if !ok {
			err = chnl.ErrMissingInCfg(curVia.ID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		nextID := curSt.(state.Prod).Next()
		newVia := chnl.Root{
			ID:      id.New(),
			Key:     curVia.Key,
			PreID:   &curVia.ID,
			StateID: &nextID,
		}
		err = s.chnls.Insert(newVia)
		if err != nil {
			s.log.Error("channel insertion failed",
				slog.Any("reason", err),
				slog.Any("via", newVia),
			)
			return err
		}
		bID, ok := term.Y.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(term.Y)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		b, ok := cfg.LookupCh(bID)
		if !ok {
			err = chnl.ErrMissingInCfg(bID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		err = s.chnls.Transfer(proc.PID, srv.PID, []chnl.ID{b.ID})
		if err != nil {
			s.log.Error("channel transfer failed",
				slog.Any("reason", err),
				slog.Any("from", proc.PID),
				slog.Any("to", srv.PID),
				slog.Any("id", b.ID),
			)
			return err
		}
		s.log.Debug("transition taking succeeded")
		recv.Cont = step.Subst(recv.Cont, recv.X, newVia.ID)
		recv.Cont = step.Subst(recv.Cont, recv.Y, b.ID)
		newProc := step.ProcRoot{
			ID:   id.New(),
			PID:  chnl.Subst(srv.PID, curVia.ID, newVia.ID),
			Term: recv.Cont,
		}
		return s.takeProc(newProc)
	case step.RecvSpec:
		viaID, ok := term.X.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(term.X)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curVia, ok := cfg.LookupCh(viaID)
		if !ok {
			err = chnl.ErrMissingInCfg(viaID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curSem, err := s.steps.SelectByVID(curVia.ID)
		if err != nil {
			s.log.Error("message selection failed",
				slog.Any("reason", err),
				slog.Any("vid", curVia.ID),
			)
			return err
		}
		if curSem == nil {
			newSrv := step.SrvRoot{
				ID:   id.New(),
				PID:  proc.PID,
				VID:  curVia.ID,
				Cont: term,
			}
			err = s.steps.Insert(newSrv)
			if err != nil {
				s.log.Error("service insertion failed",
					slog.Any("reason", err),
					slog.Any("srv", newSrv),
				)
				return err
			}
			s.log.Debug("transition taking half done", slog.Any("srv", newSrv))
			return nil
		}
		msg, ok := curSem.(step.MsgRoot)
		if !ok {
			err = step.ErrRootTypeMismatch(curSem, step.MsgRoot{})
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				// TODO log some ID
			)
			return err
		}
		send, ok := msg.Val.(step.SendSpec)
		if !ok {
			err = fmt.Errorf("val type mismatch: want %T, got %T", send, msg.Val)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				slog.Any("val", msg.Val),
			)
			return err
		}
		curSt, ok := cfg.LookupSt(curVia.ID)
		if !ok {
			err = chnl.ErrMissingInCfg(curVia.ID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		nextID := curSt.(state.Prod).Next()
		newVia := chnl.Root{
			ID:      id.New(),
			Key:     curVia.Key,
			PreID:   &curVia.ID,
			StateID: &nextID,
		}
		err = s.chnls.Insert(newVia)
		if err != nil {
			s.log.Error("channel insertion failed",
				slog.Any("reason", err),
				slog.Any("via", newVia),
			)
			return err
		}
		bID, ok := send.Y.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(send.Y)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		b, ok := cfg.LookupCh(bID)
		if !ok {
			err = chnl.ErrMissingInCfg(bID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		err = s.chnls.Transfer(msg.PID, proc.PID, []chnl.ID{b.ID})
		if err != nil {
			s.log.Error("channel transfer failed",
				slog.Any("reason", err),
				slog.Any("from", msg.PID),
				slog.Any("to", proc.PID),
				slog.Any("id", b.ID),
			)
			return err
		}
		s.log.Debug("transition taking succeeded")
		term.Cont = step.Subst(term.Cont, term.X, newVia.ID)
		term.Cont = step.Subst(term.Cont, term.Y, b.ID)
		newProc := step.ProcRoot{
			ID:   id.New(),
			PID:  chnl.Subst(proc.PID, curVia.ID, newVia.ID),
			Term: term.Cont,
		}
		return s.takeProc(newProc)
	case step.LabSpec:
		viaID, ok := term.X.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(term.X)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curVia, ok := cfg.LookupCh(viaID)
		if !ok {
			err = chnl.ErrMissingInCfg(viaID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curSem, err := s.steps.SelectByVID(curVia.ID)
		if err != nil {
			s.log.Error("service selection failed",
				slog.Any("reason", err),
				slog.Any("vid", curVia.ID),
			)
			return err
		}
		if curSem == nil {
			newMsg := step.MsgRoot{
				ID:  id.New(),
				PID: proc.PID,
				VID: curVia.ID,
				Val: term,
			}
			err = s.steps.Insert(newMsg)
			if err != nil {
				s.log.Error("message insertion failed",
					slog.Any("reason", err),
					slog.Any("msg", newMsg),
				)
				return err
			}
			s.log.Debug("transition taking half done", slog.Any("msg", newMsg))
			return nil
		}
		srv, ok := curSem.(step.SrvRoot)
		if !ok {
			err = step.ErrRootTypeMismatch(curSem, step.SrvRoot{})
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				// TODO log some ID
			)
			return err
		}
		cont, ok := srv.Cont.(step.CaseSpec)
		if !ok {
			err = fmt.Errorf("unexpected cont type: want %T, got %T", step.CaseSpec{}, srv.Cont)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				slog.Any("cont", srv.Cont),
			)
			return err
		}
		curSt, ok := cfg.LookupSt(curVia.ID)
		if !ok {
			err = chnl.ErrMissingInCfg(curVia.ID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		nextID := curSt.(state.Sum).Next(term.L)
		newVia := chnl.Root{
			ID:      id.New(),
			Key:     curVia.Key,
			PreID:   &curVia.ID,
			StateID: &nextID,
		}
		err = s.chnls.Insert(newVia)
		if err != nil {
			s.log.Error("channel insertion failed",
				slog.Any("reason", err),
				slog.Any("via", newVia),
			)
			return err
		}
		s.log.Debug("transition taking succeeded")
		newProc := step.ProcRoot{
			ID:   id.New(),
			PID:  chnl.Subst(srv.PID, curVia.ID, newVia.ID),
			Term: step.Subst(cont.Conts[term.L], cont.X, newVia.ID),
		}
		return s.takeProc(newProc)
	case step.CaseSpec:
		viaID, ok := term.X.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(term.X)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curVia, ok := cfg.LookupCh(viaID)
		if !ok {
			err = chnl.ErrMissingInCfg(viaID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curSem, err := s.steps.SelectByVID(curVia.ID)
		if err != nil {
			s.log.Error("message selection failed",
				slog.Any("reason", err),
				slog.Any("vid", curVia.ID),
			)
			return err
		}
		if curSem == nil {
			newSrv := step.SrvRoot{
				ID:   id.New(),
				PID:  proc.PID,
				VID:  curVia.ID,
				Cont: term,
			}
			err = s.steps.Insert(newSrv)
			if err != nil {
				s.log.Error("service insertion failed",
					slog.Any("reason", err),
					slog.Any("srv", newSrv),
				)
				return err
			}
			s.log.Debug("transition taking half done", slog.Any("srv", newSrv))
			return nil
		}
		msg, ok := curSem.(step.MsgRoot)
		if !ok {
			err = step.ErrRootTypeMismatch(curSem, step.MsgRoot{})
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				// TODO log some ID
			)
			return err
		}
		lab, ok := msg.Val.(step.LabSpec)
		if !ok {
			err = fmt.Errorf("val type mismatch: want %T, got %T", lab, msg.Val)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
				slog.Any("val", msg.Val),
			)
			return err
		}
		curSt, ok := cfg.LookupSt(curVia.ID)
		if !ok {
			err = chnl.ErrMissingInCfg(curVia.ID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		nextID := curSt.(state.Sum).Next(lab.L)
		newVia := chnl.Root{
			ID:      id.New(),
			Key:     curVia.Key,
			PreID:   &curVia.ID,
			StateID: &nextID,
		}
		err = s.chnls.Insert(newVia)
		if err != nil {
			s.log.Error("channel insertion failed",
				slog.Any("reason", err),
				slog.Any("via", newVia),
			)
			return err
		}
		s.log.Debug("transition taking succeeded")
		newProc := step.ProcRoot{
			ID:   id.New(),
			PID:  chnl.Subst(proc.PID, curVia.ID, newVia.ID),
			Term: step.Subst(term.Conts[lab.L], term.X, newVia.ID),
		}
		return s.takeProc(newProc)
	case step.SpawnSpec:
		newPE, err := s.Involve(PartSpec{
			Steward:   proc.PID,
			Servant:   term.PoolQN,
			Service:   term.SigID,
			Resources: term.Ys2,
		})
		if err != nil {
			return err
		}
		err = s.pools.Transfer(id.Empty(), proc.PID, []chnl.ID{newPE.ID})
		if err != nil {
			s.log.Error("channel transfer failed",
				slog.Any("reason", err),
				slog.Any("from", id.Empty()),
				slog.Any("to", proc.PID),
				slog.Any("pid", newPE.ID),
			)
			return err
		}
		s.log.Debug("transition taking succeeded")
		cfg.Add(newPE)
		cfg.Remove(term.Ys2...)
		proc.Term = step.Subst(term.Cont, term.X, newPE.ID)
		return s.takeProcWith(proc, cfg)
	case step.FwdSpec:
		viaID, ok := term.X.(chnl.ID)
		if !ok {
			err := chnl.ErrNotAnID(term.X)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curVia, ok := cfg.LookupCh(viaID)
		if !ok {
			err = chnl.ErrMissingInCfg(viaID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curSt, ok := cfg.LookupSt(curVia.ID)
		if !ok {
			err = chnl.ErrMissingInCfg(curVia.ID)
			s.log.Error("transition taking failed",
				slog.Any("reason", err),
			)
			return err
		}
		curSem, err := s.steps.SelectByVID(curVia.ID)
		if err != nil {
			s.log.Error("step selection failed",
				slog.Any("reason", err),
				slog.Any("vid", curVia.ID),
			)
			return err
		}
		switch curSt.Pol() {
		case pol.Pos:
			switch sem := curSem.(type) {
			case step.SrvRoot:
				xID, ok := term.X.(chnl.ID)
				if !ok {
					err := chnl.ErrNotAnID(term.X)
					s.log.Error("transition taking failed", slog.Any("reason", err))
					return err
				}
				x, ok := cfg.LookupCh(xID)
				if !ok {
					err = chnl.ErrMissingInCfg(xID)
					s.log.Error("transition taking failed", slog.Any("reason", err))
					return err
				}
				err := s.chnls.Transfer(proc.PID, sem.PID, []chnl.ID{x.ID})
				if err != nil {
					s.log.Error("channel transfer failed",
						slog.Any("reason", err),
						slog.Any("from", proc.PID),
						slog.Any("to", sem.PID),
						slog.Any("id", x.ID),
					)
					return err
				}
				newProc := step.ProcRoot{
					ID:   id.New(),
					PID:  sem.PID,
					Term: step.Subst(sem.Cont, term.Y, x.ID),
				}
				s.log.Debug("transition taking succeeded")
				return s.takeProc(newProc)
			case step.MsgRoot:
				yID, ok := term.Y.(chnl.ID)
				if !ok {
					err := chnl.ErrNotAnID(term.Y)
					s.log.Error("transition taking failed", slog.Any("reason", err))
					return err
				}
				y, ok := cfg.LookupCh(yID)
				if !ok {
					err = chnl.ErrMissingInCfg(yID)
					s.log.Error("transition taking failed", slog.Any("reason", err))
					return err
				}
				err := s.chnls.Transfer(proc.PID, sem.PID, []chnl.ID{y.ID})
				if err != nil {
					s.log.Error("channel transfer failed",
						slog.Any("reason", err),
						slog.Any("from", proc.PID),
						slog.Any("to", sem.PID),
						slog.Any("id", y.ID),
					)
					return err
				}
				newProc := step.ProcRoot{
					ID:   id.New(),
					PID:  sem.PID,
					Term: step.Subst(sem.Val, term.X, y.ID),
				}
				s.log.Debug("transition taking succeeded")
				return s.takeProc(newProc)
			case nil:
				newMsg := step.MsgRoot{
					ID:  id.New(),
					PID: proc.PID,
					VID: curVia.ID,
					Val: term,
				}
				err := s.steps.Insert(newMsg)
				if err != nil {
					s.log.Error("message insertion failed",
						slog.Any("reason", err),
						slog.Any("msg", newMsg),
					)
					return err
				}
				s.log.Debug("transition taking half done", slog.Any("msg", newMsg))
				return nil
			default:
				panic(step.ErrRootTypeUnexpected(curSem))
			}
		case pol.Neg:
			switch sem := curSem.(type) {
			case step.SrvRoot:
				yID, ok := term.Y.(chnl.ID)
				if !ok {
					err := chnl.ErrNotAnID(term.Y)
					s.log.Error("transition taking failed", slog.Any("reason", err))
					return err
				}
				y, ok := cfg.LookupCh(yID)
				if !ok {
					err = chnl.ErrMissingInCfg(yID)
					s.log.Error("transition taking failed", slog.Any("reason", err))
					return err
				}
				err := s.chnls.Transfer(proc.PID, sem.PID, []chnl.ID{y.ID})
				if err != nil {
					s.log.Error("channel transfer failed",
						slog.Any("reason", err),
						slog.Any("from", proc.PID),
						slog.Any("to", sem.PID),
						slog.Any("id", y.ID),
					)
					return err
				}
				newProc := step.ProcRoot{
					ID:   id.New(),
					PID:  sem.PID,
					Term: step.Subst(sem.Cont, term.X, y.ID),
				}
				s.log.Debug("transition taking succeeded")
				return s.takeProc(newProc)
			case step.MsgRoot:
				xID, ok := term.X.(chnl.ID)
				if !ok {
					err := chnl.ErrNotAnID(term.X)
					s.log.Error("transition taking failed", slog.Any("reason", err))
					return err
				}
				x, ok := cfg.LookupCh(xID)
				if !ok {
					err = chnl.ErrMissingInCfg(xID)
					s.log.Error("transition taking failed", slog.Any("reason", err))
					return err
				}
				err := s.chnls.Transfer(proc.PID, sem.PID, []chnl.ID{x.ID})
				if err != nil {
					s.log.Error("channel transfer failed",
						slog.Any("reason", err),
						slog.Any("from", proc.PID),
						slog.Any("to", sem.PID),
						slog.Any("id", x.ID),
					)
					return err
				}
				newProc := step.ProcRoot{
					ID:   id.New(),
					PID:  sem.PID,
					Term: step.Subst(sem.Val, term.Y, x.ID),
				}
				s.log.Debug("transition taking succeeded")
				return s.takeProc(newProc)
			case nil:
				newSrv := step.SrvRoot{
					ID:   id.New(),
					PID:  proc.PID,
					VID:  curVia.ID,
					Cont: term,
				}
				err = s.steps.Insert(newSrv)
				if err != nil {
					s.log.Error("service insertion failed",
						slog.Any("reason", err),
						slog.Any("srv", newSrv),
					)
					return err
				}
				s.log.Debug("transition taking half done", slog.Any("srv", newSrv))
				return nil
			default:
				panic(step.ErrRootTypeUnexpected(curSem))
			}
		default:
			panic(state.ErrPolarityUnexpected(curSt))
		}
	default:
		panic(step.ErrTermTypeUnexpected(proc.Term))
	}
}

type repo interface {
	Insert(Root) error
	SelectAll() ([]Ref, error)
	SelectByID(ID) (Root, error)
	SelectChildren(ID) ([]Ref, error)
	SelectSigs(ID) ([]sig.Ref, error)
}

// Participation aka lightweight Spawn
type PartSpec struct {
	Deal      id.ADT
	Owner     chnl.ID
	Steward   pool.ID
	Servant   pool.ID
	Service   sig.ID
	Resources []chnl.ID
}

// Transition
type TranSpec struct {
	Deal   id.ADT
	ProcID chnl.ID
	ChnlAK ak.ADT
	Term   step.Term
}

func (s *service) checkState(
	env Environment,
	ctx state.Context,
	pe state.EP,
	t step.Term,
) error {
	if pe.Z == t.Via() {
		return s.checkProvider(env, ctx, pe, t)
	}
	return s.checkClient(env, ctx, pe, t)
}

// aka checkExp
func (s *service) checkProvider(
	env Environment,
	ctx state.Context,
	pe state.EP,
	t step.Term,
) error {
	viaAttr := slog.Any("via", t.Via())
	switch term := t.(type) {
	case step.CloseSpec:
		// check ctx
		if len(ctx.Linear) > 0 {
			err := fmt.Errorf("context mismatch: want 0 items, got %v items", len(ctx.Linear))
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check via
		return state.CheckRoot(pe.C, state.OneRoot{})
	case step.WaitSpec:
		err := step.ErrTermTypeMismatch(t, step.CloseSpec{})
		s.log.Error("type checking failed", viaAttr)
		return err
	case step.SendSpec:
		// check via
		wantSt, ok := pe.C.(state.TensorRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(pe.C, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check value
		gotB, ok := ctx.Linear[term.Y]
		if !ok {
			err := chnl.ErrMissingInCtx(term.Y)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		err := state.CheckRoot(gotB, wantSt.B)
		if err != nil {
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// no cont to check
		delete(ctx.Linear, term.Y)
		pe.C = wantSt.C
		return nil
	case step.RecvSpec:
		// check via
		wantSt, ok := pe.C.(state.LolliRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(pe.C, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check value
		gotY, ok := ctx.Linear[term.Y]
		if !ok {
			err := chnl.ErrMissingInCtx(term.Y)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		err := state.CheckRoot(gotY, wantSt.Y)
		if err != nil {
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check cont
		ctx.Linear[term.Y] = wantSt.Y
		pe.C = wantSt.Z
		return s.checkState(env, ctx, pe, term.Cont)
	case step.LabSpec:
		// check via
		wantSt, ok := pe.C.(state.PlusRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(pe.C, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check label
		_, ok = wantSt.Choices[term.L]
		if !ok {
			err := fmt.Errorf("label mismatch: want %v, got %q", maps.Keys(wantSt.Choices), term.L)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// no cont to check
		return nil
	case step.CaseSpec:
		// check via
		wantSt, ok := pe.C.(state.WithRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(pe.C, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check conts
		if len(term.Conts) != len(wantSt.Choices) {
			err := fmt.Errorf("state mismatch: want %v choices, got %v conts", len(wantSt.Choices), len(term.Conts))
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		for wantL, wantChoice := range wantSt.Choices {
			gotCont, ok := term.Conts[wantL]
			if !ok {
				err := fmt.Errorf("label mismatch: want %q, got nothing", wantL)
				s.log.Error("type checking failed", viaAttr)
				return err
			}
			pe.C = wantChoice
			err := s.checkState(env, ctx, pe, gotCont)
			if err != nil {
				s.log.Error("type checking failed", viaAttr)
				return err
			}
		}
		return nil
	case step.FwdSpec:
		if len(ctx.Linear) != 1 {
			err := fmt.Errorf("context mismatch: want 1 item, got %v items", len(ctx.Linear))
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		gotD, ok := ctx.Linear[term.Y]
		if !ok {
			err := chnl.ErrMissingInCtx(term.Y)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		if gotD.Pol() != pe.C.Pol() {
			err := state.ErrPolarityMismatch(gotD, pe.C)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		return state.CheckRoot(gotD, pe.C)
	default:
		panic(step.ErrTermTypeUnexpected(t))
	}
}

func (s *service) checkClient(
	env Environment,
	ctx state.Context,
	pe state.EP,
	t step.Term,
) error {
	viaAttr := slog.Any("via", t.Via())
	switch got := t.(type) {
	case step.CloseSpec:
		err := step.ErrTermTypeMismatch(t, step.WaitSpec{})
		s.log.Error("type checking failed", viaAttr)
		return err
	case step.WaitSpec:
		// check via
		gotX, ok := ctx.Linear[got.X]
		if !ok {
			err := chnl.ErrMissingInCtx(got.X)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		wantSt, ok := gotX.(state.OneRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(gotX, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check cont
		delete(ctx.Linear, got.X)
		return s.checkState(env, ctx, pe, got.Cont)
	case step.SendSpec:
		// check via
		gotA, ok := ctx.Linear[got.X]
		if !ok {
			err := chnl.ErrMissingInCtx(got.X)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		wantSt, ok := gotA.(state.LolliRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(gotA, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check value
		gotB, ok := ctx.Linear[got.Y]
		if !ok {
			err := chnl.ErrMissingInCtx(got.Y)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		err := state.CheckRoot(gotB, wantSt.Y)
		if err != nil {
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// no cont to check
		delete(ctx.Linear, got.Y)
		ctx.Linear[got.X] = wantSt.Z
		return nil
	case step.RecvSpec:
		// check via
		gotX, ok := ctx.Linear[got.X]
		if !ok {
			err := chnl.ErrMissingInCtx(got.X)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		wantSt, ok := gotX.(state.TensorRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(gotX, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check value
		gotY, ok := ctx.Linear[got.Y]
		if !ok {
			err := chnl.ErrMissingInCtx(got.Y)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		err := state.CheckRoot(gotY, wantSt.B)
		if err != nil {
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check cont
		ctx.Linear[got.Y] = wantSt.B
		pe.C = wantSt.C
		return s.checkState(env, ctx, pe, got.Cont)
	case step.LabSpec:
		// check via
		gotA, ok := ctx.Linear[got.X]
		if !ok {
			err := chnl.ErrMissingInCtx(got.X)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		wantSt, ok := gotA.(state.WithRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(gotA, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check label
		_, ok = wantSt.Choices[got.L]
		if !ok {
			err := fmt.Errorf("label mismatch: want %v, got %q", maps.Keys(wantSt.Choices), got.L)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// no cont to check
		return nil
	case step.CaseSpec:
		// check via
		gotX, ok := ctx.Linear[got.X]
		if !ok {
			err := chnl.ErrMissingInCtx(got.X)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		wantSt, ok := gotX.(state.PlusRoot)
		if !ok {
			err := state.ErrRootTypeMismatch(gotX, wantSt)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		// check conts
		if len(got.Conts) != len(wantSt.Choices) {
			err := fmt.Errorf("state mismatch: want %v choices, got %v conts", len(wantSt.Choices), len(got.Conts))
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		for wantL, wantChoice := range wantSt.Choices {
			gotCont, ok := got.Conts[wantL]
			if !ok {
				err := fmt.Errorf("label mismatch: want %q, got nothing", wantL)
				s.log.Error("type checking failed", viaAttr)
				return err
			}
			ctx.Linear[got.X] = wantChoice
			err := s.checkState(env, ctx, pe, gotCont)
			if err != nil {
				s.log.Error("type checking failed", viaAttr)
				return err
			}
		}
		return nil
	case step.SpawnSpec:
		if !env.Contains(got.SigID) {
			err := sig.ErrRootMissingInEnv(got.SigID)
			s.log.Error("type checking failed", viaAttr)
			return err
		}
		wantCEs := env.LookupCEs(got.SigID)
		if len(got.Ys2) != len(wantCEs) {
			err := fmt.Errorf("context mismatch: want %v items, got %v items", len(wantCEs), len(got.Ys2))
			s.log.Error("type checking failed", viaAttr, slog.Any("want", wantCEs), slog.Any("got", got.Ys2))
			return err
		}
		if len(got.Ys2) == 0 {
			return nil
		}
		for i, gotCE := range got.Ys2 {
			gotSt := ctx.Linear[gotCE]
			err := state.CheckRoot(gotSt, wantCEs[i].C)
			if err != nil {
				s.log.Error("type checking failed", viaAttr, slog.Any("want", wantCEs[i]), slog.Any("got", gotCE))
				return err
			}
			delete(ctx.Linear, gotCE)
		}
		ctx.Linear[got.X] = env.LookupPE(got.SigID).C
		return s.checkState(env, ctx, pe, got.Cont)
	default:
		panic(step.ErrTermTypeUnexpected(t))
	}
}

func convertToCfg(chnls []chnl.Root) map[chnl.ID]chnl.Root {
	cfg := make(map[chnl.ID]chnl.Root, len(chnls))
	for _, ch := range chnls {
		cfg[ch.ID] = ch
	}
	return cfg
}

func convertToCtx(chnls []chnl.Root, states map[state.ID]state.Root) state.Context {
	linear := make(map[ph.ADT]state.Root, len(chnls))
	for _, ch := range chnls {
		linear[ch.ID] = states[*ch.StateID]
	}
	return state.Context{Linear: linear}
}

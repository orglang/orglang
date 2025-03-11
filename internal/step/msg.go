package step

import (
	"fmt"

	validation "github.com/go-ozzo/ozzo-validation/v4"

	"smecalculus/rolevod/lib/ak"
	"smecalculus/rolevod/lib/core"
	"smecalculus/rolevod/lib/id"
	"smecalculus/rolevod/lib/ph"
)

type RefMsg struct {
	ID string `json:"id" param:"id"`
}

type StepKind string

const (
	Proc = StepKind("proc")
	Msg  = StepKind("msg")
	Srv  = StepKind("srv")
)

var stepKindRequired = []validation.Rule{
	validation.Required,
	validation.In(Proc, Msg, Srv),
}

type RootMsg struct {
	ID string   `json:"id"`
	K  StepKind `json:"kind"`
}

type ProcRootMsg struct {
	ID   string   `json:"id"`
	PID  string   `json:"pid"`
	Term *TermMsg `json:"term"`
}

type TermKind string

const (
	Close = TermKind("close")
	Wait  = TermKind("wait")
	Send  = TermKind("send")
	Recv  = TermKind("recv")
	Lab   = TermKind("lab")
	Case  = TermKind("case")
	CTA   = TermKind("cta")
	Link  = TermKind("link")
	Spawn = TermKind("spawn")
	Fwd   = TermKind("fwd")
)

var termKindRequired = []validation.Rule{
	validation.Required,
	validation.In(Close, Wait, Send, Recv, Lab, Case, Spawn, Fwd, CTA),
}

type TermMsg struct {
	K     TermKind  `json:"kind"`
	Close *CloseMsg `json:"close,omitempty"`
	Wait  *WaitMsg  `json:"wait,omitempty"`
	Send  *SendMsg  `json:"send,omitempty"`
	Recv  *RecvMsg  `json:"recv,omitempty"`
	Lab   *LabMsg   `json:"lab,omitempty"`
	Case  *CaseMsg  `json:"case,omitempty"`
	Spawn *SpawnMsg `json:"spawn,omitempty"`
	Fwd   *FwdMsg   `json:"fwd,omitempty"`
	CTA   *CTAMsg   `json:"cta,omitempty"`
}

func (dto TermMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.K, termKindRequired...),
		validation.Field(&dto.Close, validation.Required.When(dto.K == Close)),
		validation.Field(&dto.Wait, validation.Required.When(dto.K == Wait)),
		validation.Field(&dto.Send, validation.Required.When(dto.K == Send)),
		validation.Field(&dto.Recv, validation.Required.When(dto.K == Recv)),
		validation.Field(&dto.Lab, validation.Required.When(dto.K == Lab)),
		validation.Field(&dto.Case, validation.Required.When(dto.K == Case)),
		validation.Field(&dto.Spawn, validation.Required.When(dto.K == Spawn)),
		validation.Field(&dto.Fwd, validation.Required.When(dto.K == Fwd)),
		validation.Field(&dto.CTA, validation.Required.When(dto.K == CTA)),
	)
}

type CloseMsg struct {
	A ph.Msg `json:"a"`
}

func (dto CloseMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.A, validation.Required),
	)
}

type WaitMsg struct {
	X    ph.Msg  `json:"x"`
	Cont TermMsg `json:"cont"`
}

func (dto WaitMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.X, validation.Required),
		validation.Field(&dto.Cont, validation.Required),
	)
}

type SendMsg struct {
	A ph.Msg `json:"a"`
	B ph.Msg `json:"b"`
}

func (dto SendMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.A, validation.Required),
		validation.Field(&dto.B, validation.Required),
	)
}

type RecvMsg struct {
	X    ph.Msg  `json:"x"`
	Y    ph.Msg  `json:"y"`
	Cont TermMsg `json:"cont"`
}

func (dto RecvMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.X, validation.Required),
		validation.Field(&dto.Y, validation.Required),
		validation.Field(&dto.Cont, validation.Required),
	)
}

type LabMsg struct {
	A     ph.Msg `json:"a"`
	Label string `json:"label"`
}

func (dto LabMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.A, validation.Required),
		validation.Field(&dto.Label, core.NameRequired...),
	)
}

type CaseMsg struct {
	X   ph.Msg      `json:"x"`
	Brs []BranchMsg `json:"branches"`
}

func (dto CaseMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.X, validation.Required),
		validation.Field(&dto.Brs,
			validation.Required,
			validation.Length(1, 10),
			validation.Each(validation.Required),
		),
	)
}

type BranchMsg struct {
	Label string  `json:"label"`
	Cont  TermMsg `json:"cont"`
}

func (dto BranchMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.Label, core.NameRequired...),
		validation.Field(&dto.Cont, validation.Required),
	)
}

type SpawnMsg struct {
	PE   ph.Msg   `json:"pe"`
	CEs  []string `json:"ces"`
	Cont TermMsg  `json:"cont"`
	Sig  string   `json:"sig_id"`
}

func (dto SpawnMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.PE, validation.Required),
		validation.Field(&dto.CEs, core.CtxOptional...),
		validation.Field(&dto.Cont, validation.Required),
		validation.Field(&dto.Sig, id.Required...),
	)
}

type FwdMsg struct {
	C ph.Msg `json:"c"`
	D ph.Msg `json:"d"`
}

func (dto FwdMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.C, validation.Required),
		validation.Field(&dto.D, validation.Required),
	)
}

type CTAMsg struct {
	AK  string `json:"access_key"`
	Sig string `json:"sig_id"`
}

func (dto CTAMsg) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.AK, id.Required...),
		validation.Field(&dto.Sig, id.Required...),
	)
}

// goverter:variables
// goverter:output:format assign-variable
// goverter:extend smecalculus/rolevod/lib/id:Convert.*
// goverter:extend smecalculus/rolevod/internal/chnl:Msg.*
// goverter:extend MsgFromTerm
// goverter:extend MsgToTerm
// goverter:extend MsgFromTermNilable
// goverter:extend MsgToTermNilable
var (
	MsgFromProcRoot func(ProcRoot) ProcRootMsg
	MsgToProcRoot   func(ProcRootMsg) (ProcRoot, error)
)

func MsgFromTermNilable(t Term) *TermMsg {
	if t == nil {
		return nil
	}
	term := MsgFromTerm(t)
	return &term
}

func MsgFromTerm(t Term) TermMsg {
	switch term := t.(type) {
	case CloseSpec:
		return TermMsg{
			K: Close,
			Close: &CloseMsg{
				A: ph.MsgFromPH(term.X),
			},
		}
	case WaitSpec:
		return TermMsg{
			K: Wait,
			Wait: &WaitMsg{
				X:    ph.MsgFromPH(term.X),
				Cont: MsgFromTerm(term.Cont),
			},
		}
	case SendSpec:
		return TermMsg{
			K: Send,
			Send: &SendMsg{
				A: ph.MsgFromPH(term.X),
				B: ph.MsgFromPH(term.Y),
			},
		}
	case RecvSpec:
		return TermMsg{
			K: Recv,
			Recv: &RecvMsg{
				X:    ph.MsgFromPH(term.X),
				Y:    ph.MsgFromPH(term.Y),
				Cont: MsgFromTerm(term.Cont),
			},
		}
	case LabSpec:
		return TermMsg{
			K: Lab,
			Lab: &LabMsg{
				A:     ph.MsgFromPH(term.X),
				Label: string(term.L),
			},
		}
	case CaseSpec:
		brs := []BranchMsg{}
		for l, t := range term.Conts {
			brs = append(brs, BranchMsg{Label: string(l), Cont: MsgFromTerm(t)})
		}
		return TermMsg{
			K: Case,
			Case: &CaseMsg{
				X:   ph.MsgFromPH(term.X),
				Brs: brs,
			},
		}
	case SpawnSpec:
		return TermMsg{
			K: Spawn,
			Spawn: &SpawnMsg{
				PE:   ph.MsgFromPH(term.X),
				CEs:  id.ConvertToStrings(term.Ys2),
				Cont: MsgFromTerm(term.Cont),
				Sig:  term.SigID.String(),
			},
		}
	case FwdSpec:
		return TermMsg{
			K: Fwd,
			Fwd: &FwdMsg{
				C: ph.MsgFromPH(term.X),
				D: ph.MsgFromPH(term.Y),
			},
		}
	case CTASpec:
		return TermMsg{
			K: CTA,
			CTA: &CTAMsg{
				Sig: term.Sig.String(),
				AK:  ak.ConvertToString(term.AK),
			},
		}
	default:
		panic(ErrTermTypeUnexpected(term))
	}
}

func MsgToTermNilable(dto *TermMsg) (Term, error) {
	if dto == nil {
		return nil, nil
	}
	return MsgToTerm(*dto)
}

func MsgToTerm(dto TermMsg) (Term, error) {
	switch dto.K {
	case Close:
		a, err := ph.MsgToPH(dto.Close.A)
		if err != nil {
			return nil, err
		}
		return CloseSpec{X: a}, nil
	case Wait:
		x, err := ph.MsgToPH(dto.Wait.X)
		if err != nil {
			return nil, err
		}
		cont, err := MsgToTerm(dto.Wait.Cont)
		if err != nil {
			return nil, err
		}
		return WaitSpec{X: x, Cont: cont}, nil
	case Send:
		a, err := ph.MsgToPH(dto.Send.A)
		if err != nil {
			return nil, err
		}
		b, err := ph.MsgToPH(dto.Send.B)
		if err != nil {
			return nil, err
		}
		return SendSpec{X: a, Y: b}, nil
	case Recv:
		x, err := ph.MsgToPH(dto.Recv.X)
		if err != nil {
			return nil, err
		}
		y, err := ph.MsgToPH(dto.Recv.Y)
		if err != nil {
			return nil, err
		}
		cont, err := MsgToTerm(dto.Recv.Cont)
		if err != nil {
			return nil, err
		}
		return RecvSpec{X: x, Y: y, Cont: cont}, nil
	case Lab:
		a, err := ph.MsgToPH(dto.Lab.A)
		if err != nil {
			return nil, err
		}
		return LabSpec{X: a, L: core.Label(dto.Lab.Label)}, nil
	case Case:
		x, err := ph.MsgToPH(dto.Case.X)
		if err != nil {
			return nil, err
		}
		conts := make(map[core.Label]Term, len(dto.Case.Brs))
		for _, b := range dto.Case.Brs {
			cont, err := MsgToTerm(b.Cont)
			if err != nil {
				return nil, err
			}
			conts[core.Label(b.Label)] = cont
		}
		return CaseSpec{X: x, Conts: conts}, nil
	case Spawn:
		pe, err := ph.MsgToPH(dto.Spawn.PE)
		if err != nil {
			return nil, err
		}
		ces, err := id.ConvertFromStrings(dto.Spawn.CEs)
		if err != nil {
			return nil, err
		}
		cont, err := MsgToTerm(dto.Spawn.Cont)
		if err != nil {
			return nil, err
		}
		sigID, err := id.ConvertFromString(dto.Spawn.Sig)
		if err != nil {
			return nil, err
		}
		return SpawnSpec{X: pe, Ys2: ces, Cont: cont, SigID: sigID}, nil
	case Fwd:
		c, err := ph.MsgToPH(dto.Fwd.C)
		if err != nil {
			return nil, err
		}
		d, err := ph.MsgToPH(dto.Fwd.D)
		if err != nil {
			return nil, err
		}
		return FwdSpec{X: c, Y: d}, nil
	case CTA:
		key, err := ak.ConvertFromString(dto.CTA.AK)
		if err != nil {
			return nil, err
		}
		sigID, err := id.ConvertFromString(dto.CTA.Sig)
		if err != nil {
			return nil, err
		}
		return CTASpec{AK: key, Sig: sigID}, nil
	default:
		panic(ErrUnexpectedTermKind(dto.K))
	}
}

func ErrUnexpectedTermKind(k TermKind) error {
	return fmt.Errorf("unexpected term kind: %v", k)
}

func ErrUnexpectedStepKind(k StepKind) error {
	return fmt.Errorf("unexpected step kind: %v", k)
}

package termdef

import (
	"orglang/go-engine/lib/db"
)

type Repo interface {
	InsertProc(db.UoW, DefRec) error
}

type ExpRecDS struct {
	K     expKind     `json:"k"`
	Close *closeRecDS `json:"close,omitempty"`
	Wait  *waitRecDS  `json:"wait,omitempty"`
	Send  *sendRecDS  `json:"send,omitempty"`
	Recv  *recvRecDS  `json:"recv,omitempty"`
	Lab   *labRecDS   `json:"lab,omitempty"`
	Case  *caseRecDS  `json:"case,omitempty"`
	Fwd   *fwdRecDS   `json:"fwd,omitempty"`
}

type ExpSpecDS struct {
	K     expKind      `json:"k"`
	Close *closeSpecDS `json:"close,omitempty"`
	Wait  *waitSpecDS  `json:"wait,omitempty"`
	Send  *sendSpecDS  `json:"send,omitempty"`
	Recv  *recvSpecDS  `json:"recv,omitempty"`
	Lab   *labSpecDS   `json:"lab,omitempty"`
	Case  *caseSpecDS  `json:"case,omitempty"`
	Fwd   *fwdSpecDS   `json:"fwd,omitempty"`
}

type expKind int

const (
	unkKind expKind = iota
	closeExp
	waitExp
	sendExp
	recvExp
	labExp
	caseExp
	linkExp
	spawnExp
	fwdExp
)

type closeSpecDS struct {
	X string `json:"x"`
}

type closeRecDS struct {
	X string `json:"x"`
}

type waitSpecDS struct {
	X      string    `json:"x"`
	ContES ExpSpecDS `json:"cont"`
}

type waitRecDS struct {
	X      string    `json:"x"`
	ContES ExpSpecDS `json:"cont"`
}

type sendSpecDS struct {
	X string `json:"x"`
	Y string `json:"y"`
}

type sendRecDS struct {
	X string `json:"x"`
	A string `json:"a"`
	B string `json:"b"`
}

type recvSpecDS struct {
	X      string    `json:"x"`
	Y      string    `json:"y"`
	ContES ExpSpecDS `json:"cont"`
}

type recvRecDS struct {
	X      string    `json:"x"`
	A      string    `json:"a"`
	Y      string    `json:"y"`
	ContES ExpSpecDS `json:"cont"`
}

type labSpecDS struct {
	X     string `json:"x"`
	Label string `json:"lab"`
}

type labRecDS struct {
	X     string `json:"x"`
	Label string `json:"lab"`
}

type caseSpecDS struct {
	X        string         `json:"x"`
	Branches []branchSpecDS `json:"brs"`
}

type caseRecDS struct {
	X        string        `json:"x"`
	Branches []branchRecDS `json:"brs"`
}

type branchSpecDS struct {
	Label  string    `json:"lab"`
	ContES ExpSpecDS `json:"cont"`
}

type branchRecDS struct {
	Label  string    `json:"lab"`
	ContES ExpSpecDS `json:"cont"`
}

type fwdSpecDS struct {
	X string `json:"x"`
	Y string `json:"y"`
}

type fwdRecDS struct {
	X string `json:"x"`
	B string `json:"b"`
}

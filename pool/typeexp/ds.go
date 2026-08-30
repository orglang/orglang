package typeexp

import (
	"orglang/go-engine/lib/db"

	"orglang/go-engine/adt/symbol"
	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/adt/valkey"
)

type Repo interface {
	AddRec(db.UoW, ExpRec, typesem.SemRef) error
	GetRecByVK(db.UoW, valkey.ADT) (ExpRec, error)
	GetRecsByVKs(db.UoW, []valkey.ADT) ([]ExpRec, error)
	GetRecMap(db.UoW, map[symbol.ADT]valkey.ADT) (map[symbol.ADT]ExpRec, error)
}

type expKind int16

const (
	unkKind expKind = iota
	oneKind
	linkKind
	tensorKind
	lolliKind
	plusKind
	withKind
	upKind
	downKind
)

type expRefDS struct {
	ExpVK int64   `db:"exp_vk" json:"exp_vk"`
	K     expKind `db:"kind" json:"kind"`
}

type expRecDS struct {
	ExpVK  int64
	States []stateDS
}

type stateDS struct {
	ExpVK    int64     `db:"exp_vk"`
	SupExpVK int64     `db:"sup_exp_vk"`
	K        expKind   `db:"kind"`
	Spec     expSpecDS `db:"spec" fieldopt:"noexpand"`
}

type expSpecDS struct {
	Link   string   `json:"link,omitempty"`
	Tensor *prodDS  `json:"tensor,omitempty"`
	Lolli  *prodDS  `json:"lolli,omitempty"`
	Plus   *sumDS   `json:"plus,omitempty"`
	With   *sumDS   `json:"with,omitempty"`
	Up     *shiftDS `json:"up,omitempty"`
	Down   *shiftDS `json:"down,omitempty"`
}

type prodDS struct {
	ValExpVK  int64 `json:"on"`
	ContExpVK int64 `json:"to"`
}

type sumDS struct {
	ProcQNs   []string `json:"on"`
	ContExpVK int64    `json:"to"`
}

type shiftDS struct {
	ContExpVK int64 `json:"to"`
}

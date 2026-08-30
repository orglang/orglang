package e2e

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/fx"

	"orglang/go-engine/lib/e2e"

	"github.com/orglang/go-sdk/lib/rc"

	"github.com/orglang/go-sdk/adt/compsem"
	"github.com/orglang/go-sdk/adt/compvar"
	"github.com/orglang/go-sdk/adt/termvar"
	"github.com/orglang/go-sdk/pool/compexec"
	"github.com/orglang/go-sdk/pool/compstep"
	pooltermdec "github.com/orglang/go-sdk/pool/termdec"
	pooltermexp "github.com/orglang/go-sdk/pool/termexp"
	pooltypedef "github.com/orglang/go-sdk/pool/typedef"
	pooltypeexp "github.com/orglang/go-sdk/pool/typeexp"
	proccompstep "github.com/orglang/go-sdk/proc/compstep"
	proctermdec "github.com/orglang/go-sdk/proc/termdec"
	proctermexp "github.com/orglang/go-sdk/proc/termexp"
	"github.com/orglang/go-sdk/proc/typedef"
	"github.com/orglang/go-sdk/proc/typeexp"
)

func TestPool(t *testing.T) {
	s := suite{}
	s.beforeAll(t)
	t.Run("WaitClose", s.waitClose)
	// t.Run("RecvSend", s.recvSend)
	// t.Run("CaseLab", s.caseLab)
	// t.Run("Call", s.call)
	// t.Run("Fwd", s.fwd)
}

type suite struct {
	fx.In
	PoolDecAPI  e2e.PoolDecAPI
	PoolExecAPI e2e.PoolExecAPI
	XactDefAPI  e2e.XactDefAPI
	ProcDecAPI  e2e.ProcDecAPI
	ProcExecAPI e2e.ProcExecAPI
	TypeDefAPI  e2e.TypeDefAPI
	DB          *sql.DB `optional:"true"`
}

func (s *suite) beforeAll(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://orglang:orglang@localhost:5432/orglang")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := db.Close()
		if err != nil {
			t.Logf("stop failed: %s", err)
		}
	})
	app := fx.New(rc.Module, e2e.Module, fx.Populate(s))
	s.DB = db
	t.Cleanup(func() {
		err := app.Stop(t.Context())
		if err != nil {
			t.Logf("stop failed: %s", err)
		}
	})
}

func (s *suite) beforeEach(t *testing.T) {
	tables := []string{
		"pool_desc_binds", "pool_type_defs", "pool_type_exps", "pool_term_decs",
		"pool_impl_binds", "pool_comp_execs", "pool_comp_vars", "pool_comm_exchs", "pool_comm_turns",
		"proc_desc_binds", "proc_type_defs", "proc_type_exps", "proc_term_decs",
		"proc_impl_binds", "proc_comp_execs", "proc_comp_vars", "proc_comm_exchs", "proc_comm_turns",
	}
	for _, table := range tables {
		_, err := s.DB.Exec(fmt.Sprintf("truncate table %v", table))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (s *suite) waitClose(t *testing.T) {
	s.beforeEach(t)
	// given
	closerProcQN := "closer-proc-qn"
	waiterProcQN := "waiter-proc-qn"
	// and
	poolTypeQN := "pool-type-qn"
	_, err := s.XactDefAPI.Create(pooltypedef.DefSpec{
		TypeQN: poolTypeQN,
		TypeExp: pooltypeexp.ExpSpec{
			K: pooltypeexp.Up,
			Up: &pooltypeexp.ShiftSpec{
				ContExp: pooltypeexp.ExpSpec{
					K: pooltypeexp.With,
					With: &pooltypeexp.LaborSpec{
						// пул заявляет компетенции
						ProcQNs: []string{closerProcQN, waiterProcQN},
						ContExp: pooltypeexp.ExpSpec{
							K: pooltypeexp.Down,
							Down: &pooltypeexp.ShiftSpec{
								ContExp: pooltypeexp.ExpSpec{
									K:    pooltypeexp.Link,
									Link: &pooltypeexp.LinkSpec{TypeQN: poolTypeQN},
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	poolTermQN := "pool-term-qn"
	poolCompQN := "pool-comp-qn"
	poolAssetPH := "pool-asset-ph"
	poolLiabPH := "pool-liab-ph"
	_, err = s.PoolDecAPI.Create(pooltermdec.DecSpec{
		TermQN: poolTermQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: poolLiabPH,
			TypeQN: poolTypeQN,
		},
		AssetVars: []termvar.VarSpec{{
			ChnlPH: poolAssetPH,
			TypeQN: poolTypeQN,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	poolExecRef, err := s.PoolExecAPI.Create(compexec.ExecSpec{
		TermQN: poolTermQN,
		LiabVar: compvar.VarSpec{
			ChnlPH: poolLiabPH,
			TermQN: poolCompQN,
		},
		AssetVars: []compvar.VarSpec{{
			ChnlPH: poolAssetPH,
			TermQN: poolCompQN,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: poolExecRef,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Acquire, // пул запрашивает доступ к самому себе
			Acquire: &pooltermexp.AcquireSpec{
				CommChnlPH: poolAssetPH,
				ContExp: pooltermexp.ExpSpec{
					K: pooltermexp.Hire, // пул запрашивает компетенцию closerProcQN
					Hire: &pooltermexp.HireSpec{
						CommChnlPH: poolAssetPH, // пул выступает нанимателем самого себя
						ProcTermQN: closerProcQN,
						ContExp: pooltermexp.ExpSpec{
							K: pooltermexp.Release,
							Release: &pooltermexp.ReleaseSpec{
								CommChnlPH: poolAssetPH,
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: poolExecRef,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Accept, // пул одобряет доступ к самому себе
			Accept: &pooltermexp.AcceptSpec{
				CommChnlPH: poolLiabPH,
				ContExp: pooltermexp.ExpSpec{
					K: pooltermexp.Apply, // пул предлагает компетенцию closerProcQN
					Apply: &pooltermexp.ApplySpec{
						CommChnlPH: poolLiabPH, // пул выступает соискателем
						ProcTermQN: closerProcQN,
						ContExp: pooltermexp.ExpSpec{
							K: pooltermexp.Detach,
							Detach: &pooltermexp.DetachSpec{
								CommChnlPH: poolLiabPH,
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	time.Sleep(1 * time.Second)
	// and
	oneTypeQN := "one-type-qn"
	_, err = s.TypeDefAPI.Create(typedef.DefSpec{
		TypeQN:  oneTypeQN,
		TypeExp: typeexp.ExpSpec{K: typeexp.One},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	closerProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: closerProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "closer-provider-ph",
			TypeQN: oneTypeQN,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	closerProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: poolExecRef,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef: closerProcDec.TermRef,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: poolExecRef,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Acquire,
			Acquire: &pooltermexp.AcquireSpec{
				CommChnlPH: poolAssetPH,
				ContExp: pooltermexp.ExpSpec{
					K: pooltermexp.Hire,
					Hire: &pooltermexp.HireSpec{
						CommChnlPH: poolAssetPH,
						ProcTermQN: waiterProcQN,
						ContExp: pooltermexp.ExpSpec{
							K: pooltermexp.Release,
							Release: &pooltermexp.ReleaseSpec{
								CommChnlPH: poolAssetPH,
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: poolExecRef,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Accept,
			Accept: &pooltermexp.AcceptSpec{
				CommChnlPH: poolAssetPH,
				ContExp: pooltermexp.ExpSpec{
					K: pooltermexp.Apply,
					Apply: &pooltermexp.ApplySpec{
						CommChnlPH: poolLiabPH,
						ProcTermQN: waiterProcQN,
						ContExp: pooltermexp.ExpSpec{
							K: pooltermexp.Detach,
							Detach: &pooltermexp.DetachSpec{
								CommChnlPH: poolAssetPH,
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	waiterProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN:  waiterProcQN,
		LiabVar: termvar.VarSpec{ChnlPH: "waiter-provider-ph", TypeQN: oneTypeQN},
		AssetVars: []termvar.VarSpec{
			{ChnlPH: "waiter-closer-ph", TypeQN: oneTypeQN},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	waiterProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: poolExecRef,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef:  waiterProcDec.TermRef,
				ProcCompRefs: []compsem.SemRef{closerProcExec},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// when
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: closerProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Close,
			Close: &proctermexp.CloseSpec{
				CommChnlPH: closerProcDec.LiabVar.ChnlPH,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: waiterProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Wait,
			Wait: &proctermexp.WaitSpec{
				CommChnlPH: waiterProcDec.AssetVars[0].ChnlPH,
				ContES: proctermexp.ExpSpec{
					K: proctermexp.Close,
					Close: &proctermexp.CloseSpec{
						CommChnlPH: waiterProcDec.LiabVar.ChnlPH,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// then
	// TODO добавить проверку
}

func (s *suite) recvSend(t *testing.T) {
	s.beforeEach(t)
	// given
	senderProcQN := "sender-proc-qn"
	messageProcQN := "message-proc-qn"
	receiverProcQN := "receiver-proc-qn"
	// and
	withXactQN := "with-xact-qn"
	_, err := s.XactDefAPI.Create(pooltypedef.DefSpec{
		TypeQN: withXactQN,
		TypeExp: pooltypeexp.ExpSpec{
			K: pooltypeexp.Up,
			Up: &pooltypeexp.ShiftSpec{
				ContExp: pooltypeexp.ExpSpec{
					K: pooltypeexp.With,
					With: &pooltypeexp.LaborSpec{
						ProcQNs: []string{senderProcQN, receiverProcQN, messageProcQN},
						ContExp: pooltypeexp.ExpSpec{
							K: pooltypeexp.Down,
							Down: &pooltypeexp.ShiftSpec{
								ContExp: pooltypeexp.ExpSpec{
									K:    pooltypeexp.Link,
									Link: &pooltypeexp.LinkSpec{TypeQN: withXactQN},
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	myPoolQN := "my-pool-qn"
	poolProviderPH := "pool-provider-ph"
	poolClientPH := "pool-client-ph"
	_, err = s.PoolDecAPI.Create(pooltermdec.DecSpec{
		TermQN: myPoolQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: poolProviderPH,
			TypeQN: withXactQN,
		},
		AssetVars: []termvar.VarSpec{{
			ChnlPH: poolClientPH,
			TypeQN: withXactQN,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	myPoolExec, err := s.PoolExecAPI.Create(compexec.ExecSpec{
		TermQN: myPoolQN,
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	lolliTypeQN := "lolli-type-qn"
	_, err = s.TypeDefAPI.Create(typedef.DefSpec{
		TypeQN: lolliTypeQN,
		TypeExp: typeexp.ExpSpec{
			K: typeexp.Lolli,
			Lolli: &typeexp.ProdSpec{
				Val:  typeexp.ExpSpec{K: typeexp.One},
				Cont: typeexp.ExpSpec{K: typeexp.One},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	oneTypeQN := "one-type-qn"
	_, err = s.TypeDefAPI.Create(typedef.DefSpec{
		TypeQN:  oneTypeQN,
		TypeExp: typeexp.ExpSpec{K: typeexp.One},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	receiverProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: receiverProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "receiver-provider-ph",
			TypeQN: lolliTypeQN,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	messageProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: messageProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "message-provider-ph",
			TypeQN: oneTypeQN,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	senderProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: senderProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "sender-provider-ph",
			TypeQN: oneTypeQN,
		},
		AssetVars: []termvar.VarSpec{
			{ChnlPH: "sender-receiver-ph", TypeQN: lolliTypeQN},
			{ChnlPH: "sender-message-ph", TypeQN: oneTypeQN},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				CommChnlPH: poolClientPH,
				ProcTermQN: receiverProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	receiverProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef: receiverProcDec.TermRef,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				CommChnlPH: poolClientPH,
				ProcTermQN: messageProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	messageProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef: messageProcDec.TermRef,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				CommChnlPH: poolClientPH,
				ProcTermQN: senderProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	senderProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef:  senderProcDec.TermRef,
				ProcCompRefs: []compsem.SemRef{receiverProcExec, messageProcExec},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// when
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: receiverProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Recv,
			Recv: &proctermexp.RecvSpec{
				CommChnlPH: receiverProcDec.LiabVar.ChnlPH,
				BindChnlPH: "receiver-message-ph",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: senderProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Send,
			Send: &proctermexp.SendSpec{
				CommChnlPH: senderProcDec.AssetVars[0].ChnlPH,
				ValChnlPH:  senderProcDec.AssetVars[1].ChnlPH,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// then
	// TODO добавить проверку
}

func (s *suite) caseLab(t *testing.T) {
	s.beforeEach(t)
	// given
	myPoolQN := "my-pool-qn"
	myPoolExec, err := s.PoolExecAPI.Create(compexec.ExecSpec{
		TermQN: myPoolQN,
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	patternQN := "pattern-qn"
	// and
	withType, err := s.TypeDefAPI.Create(typedef.DefSpec{
		TypeQN: "with-type-qn",
		TypeExp: typeexp.ExpSpec{
			With: &typeexp.SumSpec{
				Choices: []typeexp.ChoiceSpec{
					{LabQN: patternQN, Cont: typeexp.ExpSpec{K: typeexp.One}},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	oneType, err := s.TypeDefAPI.Create(typedef.DefSpec{
		TypeQN:  "one-type-qn",
		TypeExp: typeexp.ExpSpec{K: typeexp.One},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	followerProcQN := "follower-proc-qn"
	followerProcDec := proctermdec.DecSpec{
		TermQN: followerProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "follower-provider-ph",
			TypeQN: withType.DefSpec.TypeQN,
		},
	}
	followerProcDec2, err := s.ProcDecAPI.Create(followerProcDec)
	if err != nil {
		t.Fatal(err)
	}
	// and
	deciderProcQN := "decider-proc-qn"
	deciderProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN:    deciderProcQN,
		AssetVars: []termvar.VarSpec{followerProcDec.LiabVar},
		LiabVar: termvar.VarSpec{
			ChnlPH: "decider-provider-ph",
			TypeQN: oneType.DefSpec.TypeQN,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	poolFollowerPH := "pool-follower-ph"
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				ProcTermQN: followerProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	followerProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef: followerProcDec2.TermRef,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				ProcTermQN: deciderProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	deciderProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef:  deciderProcDec.TermRef,
				ProcCompRefs: []compsem.SemRef{followerProcExec},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// when
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: followerProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Case,
			Case: &proctermexp.CaseSpec{
				CommChnlPH: poolFollowerPH,
				ContBSes: []proctermexp.BranchSpec{
					{PatternQN: patternQN, ContES: proctermexp.ExpSpec{
						K: proctermexp.Close,
						Close: &proctermexp.CloseSpec{
							CommChnlPH: poolFollowerPH,
						},
					},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: deciderProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Lab,
			Lab: &proctermexp.LabSpec{
				CommChnlPH: poolFollowerPH,
				PatternQN:  patternQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// then
	// TODO добавить проверку
}

func (s *suite) call(t *testing.T) {
	s.beforeEach(t)
	// given
	myPoolQN := "my-pool-qn"
	myPoolExec, err := s.PoolExecAPI.Create(compexec.ExecSpec{
		TermQN: myPoolQN,
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	oneType, err := s.TypeDefAPI.Create(
		typedef.DefSpec{
			TypeQN:  "one-type-qn",
			TypeExp: typeexp.ExpSpec{K: typeexp.One},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	// and
	injecteeProcQN := "injectee-proc-qn"
	injecteeProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: injecteeProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "injectee-provider-ph",
			TypeQN: oneType.DefSpec.TypeQN,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				ProcTermQN: injecteeProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	injecteeProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef: injecteeProcDec.TermRef,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	callerProcQN := "caller-proc-qn"
	callerProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: callerProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "caller-provider-ph",
			TypeQN: oneType.DefSpec.TypeQN,
		},
		AssetVars: []termvar.VarSpec{
			{ChnlPH: "caller-injectee-ph", TypeQN: oneType.DefSpec.TypeQN},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				ProcTermQN: callerProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	callerProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef:  callerProcDec.TermRef,
				ProcCompRefs: []compsem.SemRef{injecteeProcExec},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	calleeProcQN := "callee-proc-qn"
	_, err = s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: calleeProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "callee-provider-ph",
			TypeQN: oneType.DefSpec.TypeQN,
		},
		AssetVars: []termvar.VarSpec{
			{ChnlPH: "callee-injectee-ph", TypeQN: oneType.DefSpec.TypeQN},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	callerCalleePH := "caller-callee-ph"
	// when
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: callerProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Call,
			Call: &proctermexp.CallSpec{
				BindChnlPH: callerCalleePH,
				ProcTermQN: calleeProcQN,
				ValChnlPHs: []string{callerProcDec.AssetVars[0].ChnlPH},
				ContES: proctermexp.ExpSpec{
					K: proctermexp.Wait,
					Wait: &proctermexp.WaitSpec{
						CommChnlPH: callerCalleePH,
						ContES: proctermexp.ExpSpec{
							K: proctermexp.Close,
							Close: &proctermexp.CloseSpec{
								CommChnlPH: callerProcDec.LiabVar.ChnlPH,
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// then
	// TODO добавить проверку
}

func (s *suite) fwd(t *testing.T) {
	s.beforeEach(t)
	// given
	myPoolQN := "my-pool-qn"
	myPoolExec, err := s.PoolExecAPI.Create(compexec.ExecSpec{
		TermQN: myPoolQN,
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	oneType, err := s.TypeDefAPI.Create(typedef.DefSpec{
		TypeQN:  "one-type-qn",
		TypeExp: typeexp.ExpSpec{K: typeexp.One},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	closerProcQN := "closer-proc-qn"
	closerProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: closerProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "closer-provider-ph",
			TypeQN: oneType.DefSpec.TypeQN,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	forwarderProcQN := "forwarder-proc-qn"
	forwarderProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: forwarderProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "forwarder-provider-ph",
			TypeQN: oneType.DefSpec.TypeQN,
		},
		AssetVars: []termvar.VarSpec{
			{ChnlPH: "forwarder-closer-ph", TypeQN: oneType.DefSpec.TypeQN},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	waiterProcQN := "waiter-proc-qn"
	waiterProcDec, err := s.ProcDecAPI.Create(proctermdec.DecSpec{
		TermQN: waiterProcQN,
		LiabVar: termvar.VarSpec{
			ChnlPH: "waiter-provider-ph",
			TypeQN: oneType.DefSpec.TypeQN,
		},
		AssetVars: []termvar.VarSpec{
			{ChnlPH: "waiter-forwarder-ph", TypeQN: oneType.DefSpec.TypeQN},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				ProcTermQN: closerProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	closerProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef: closerProcDec.TermRef,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				ProcTermQN: forwarderProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	forwarderProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef:  forwarderProcDec.TermRef,
				ProcCompRefs: []compsem.SemRef{closerProcExec},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.PoolExecAPI.Take(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Hire,
			Hire: &pooltermexp.HireSpec{
				ProcTermQN: waiterProcQN,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waiterProcExec, err := s.PoolExecAPI.Spawn(compstep.StepSpec{
		CompRef: myPoolExec,
		PoolExp: pooltermexp.ExpSpec{
			K: pooltermexp.Spawn,
			Spawn: &pooltermexp.SpawnSpec{
				ProcTermRef:  waiterProcDec.TermRef,
				ProcCompRefs: []compsem.SemRef{forwarderProcExec},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: closerProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Close,
			Close: &proctermexp.CloseSpec{
				CommChnlPH: closerProcDec.LiabVar.ChnlPH,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// when
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: forwarderProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Fwd,
			Fwd: &proctermexp.FwdSpec{
				CommChnlPH: forwarderProcDec.LiabVar.ChnlPH,
				ContChnlPH: forwarderProcDec.AssetVars[0].ChnlPH,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// and
	err = s.ProcExecAPI.Take(proccompstep.StepSpec{
		CompRef: waiterProcExec,
		ProcExp: proctermexp.ExpSpec{
			K: proctermexp.Wait,
			Wait: &proctermexp.WaitSpec{
				CommChnlPH: waiterProcDec.AssetVars[0].ChnlPH,
				ContES: proctermexp.ExpSpec{
					K: proctermexp.Close,
					Close: &proctermexp.CloseSpec{
						CommChnlPH: waiterProcDec.LiabVar.ChnlPH,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// then
	// TODO добавить проверку
}

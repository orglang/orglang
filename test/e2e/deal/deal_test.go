package deal_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"smecalculus/rolevod/lib/core"
	"smecalculus/rolevod/lib/sym"

	"smecalculus/rolevod/internal/chnl"
	"smecalculus/rolevod/internal/state"
	"smecalculus/rolevod/internal/step"

	"smecalculus/rolevod/app/deal"
	"smecalculus/rolevod/app/role"
	"smecalculus/rolevod/app/sig"
)

var (
	roleAPI = role.NewAPI()
	sigAPI  = sig.NewAPI()
	dealAPI = deal.NewAPI()
	tc      *testCase
)

func TestMain(m *testing.M) {
	ts := testSuite{}
	tc = ts.Setup()
	code := m.Run()
	ts.Teardown()
	os.Exit(code)
}

type testSuite struct {
	db *sql.DB
}

func (ts *testSuite) Setup() *testCase {
	db, err := sql.Open("pgx", "postgres://rolevod:rolevod@localhost:5432/rolevod")
	if err != nil {
		panic(err)
	}
	ts.db = db
	return &testCase{db}
}

func (ts *testSuite) Teardown() {
	err := ts.db.Close()
	if err != nil {
		panic(err)
	}
}

type testCase struct {
	db *sql.DB
}

func (tc *testCase) Setup(t *testing.T) {
	tables := []string{
		"aliases",
		"pool_roots",
		"sig_roots", "sig_pes", "sig_ces",
		"role_roots", "role_states",
		"states", "channels", "steps", "clientships"}
	for _, table := range tables {
		_, err := tc.db.Exec(fmt.Sprintf("truncate table %v", table))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTake(t *testing.T) {

	t.Run("WaitClose", func(t *testing.T) {
		tc.Setup(t)
		// given
		oneRoleSpec := role.Spec{
			FQN:   "one-role",
			State: state.OneSpec{},
		}
		oneRole, err := roleAPI.Create(oneRoleSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		closerSigSpec := sig.Spec{
			FQN: "closer",
			PE: chnl.Spec{
				Key:  "closing-1",
				Link: oneRole.FQN,
			},
		}
		closerSig, err := sigAPI.Create(closerSigSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		waiterSigSpec := sig.Spec{
			FQN: "waiter",
			PE: chnl.Spec{
				Key:  "closing-2",
				Link: oneRole.FQN,
			},
			CEs: []chnl.Spec{
				closerSig.X2,
			},
		}
		waiterSig, err := sigAPI.Create(waiterSigSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		bigDealSpec := deal.Spec{
			Name: "big-deal",
		}
		bigDeal, err := dealAPI.Create(bigDealSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		closerSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: closerSig.ID,
		}
		closer, err := dealAPI.Involve(closerSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		waiterSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: waiterSig.ID,
			Resources: []chnl.ID{
				closer.ID,
			},
		}
		waiter, err := dealAPI.Involve(waiterSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		closeSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: closer.ID,
			Term: step.CloseSpec{
				X: closer.ID,
			},
		}
		// when
		err = dealAPI.Take(closeSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		waitSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: waiter.ID,
			Term: step.WaitSpec{
				X: closer.ID,
				Cont: step.CloseSpec{
					X: waiter.ID,
				},
			},
		}
		// and
		err = dealAPI.Take(waitSpec)
		if err != nil {
			t.Fatal(err)
		}
		// then
		// TODO добавить проверку
	})

	t.Run("RecvSend", func(t *testing.T) {
		tc.Setup(t)
		// given
		lolliRoleSpec := role.Spec{
			FQN: "lolli-role",
			State: state.LolliSpec{
				Y: state.OneSpec{},
				Z: state.OneSpec{},
			},
		}
		lolliRole, err := roleAPI.Create(lolliRoleSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneRoleSpec := role.Spec{
			FQN:   "one-role",
			State: state.OneSpec{},
		}
		oneRole, err := roleAPI.Create(oneRoleSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		lolliSigSpec := sig.Spec{
			FQN: "sig-1",
			PE: chnl.Spec{
				Key:  "chnl-1",
				Link: lolliRole.FQN,
			},
		}
		lolliSig, err := sigAPI.Create(lolliSigSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSigSpec1 := sig.Spec{
			FQN: "sig-2",
			PE: chnl.Spec{
				Key:  "chnl-2",
				Link: oneRole.FQN,
			},
		}
		oneSig1, err := sigAPI.Create(oneSigSpec1)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSigSpec2 := sig.Spec{
			FQN: "sig-3",
			PE: chnl.Spec{
				Key:  "chnl-3",
				Link: oneRole.FQN,
			},
			CEs: []chnl.Spec{
				lolliSigSpec.PE,
				oneSig1.X2,
			},
		}
		oneSig2, err := sigAPI.Create(oneSigSpec2)
		if err != nil {
			t.Fatal(err)
		}
		// and
		bigDealSpec := deal.Spec{
			Name: "deal-1",
		}
		bigDeal, err := dealAPI.Create(bigDealSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		receiverSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: lolliSig.ID,
		}
		receiver, err := dealAPI.Involve(receiverSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		messageSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: oneSig1.ID,
		}
		message, err := dealAPI.Involve(messageSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		senderSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: oneSig2.ID,
			Resources: []chnl.ID{
				receiver.ID,
				message.ID,
			},
		}
		sender, err := dealAPI.Involve(senderSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		recvSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: receiver.ID,
			Term: step.RecvSpec{
				X: receiver.ID,
				Y: message.ID,
				Cont: step.WaitSpec{
					X: message.ID,
					Cont: step.CloseSpec{
						X: receiver.ID,
					},
				},
			},
		}
		// when
		err = dealAPI.Take(recvSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		sendSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: sender.ID,
			Term: step.SendSpec{
				X: receiver.ID,
				Y: message.ID,
			},
		}
		// and
		err = dealAPI.Take(sendSpec)
		if err != nil {
			t.Fatal(err)
		}
		// then
		// TODO добавить проверку
	})

	t.Run("CaseLab", func(t *testing.T) {
		tc.Setup(t)
		// given
		label := core.Label("label-1")
		// and
		withRoleSpec := role.Spec{
			FQN: "with-role",
			State: state.WithSpec{
				Choices: map[core.Label]state.Spec{
					label: state.OneSpec{},
				},
			},
		}
		withRole, err := roleAPI.Create(withRoleSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneRoleSpec := role.Spec{
			FQN:   "one-role",
			State: state.OneSpec{},
		}
		oneRole, err := roleAPI.Create(oneRoleSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		withSigSpec := sig.Spec{
			FQN: "sig-1",
			PE: chnl.Spec{
				Key:  "chnl-1",
				Link: withRole.FQN,
			},
		}
		withSig, err := sigAPI.Create(withSigSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSigSpec := sig.Spec{
			FQN: "sig-2",
			PE: chnl.Spec{
				Key:  "chnl-2",
				Link: oneRole.FQN,
			},
			CEs: []chnl.Spec{
				withSig.X2,
			},
		}
		oneSig, err := sigAPI.Create(oneSigSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		bigDealSpec := deal.Spec{
			Name: "deal-1",
		}
		bigDeal, err := dealAPI.Create(bigDealSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		followerSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: withSig.ID,
		}
		follower, err := dealAPI.Involve(followerSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		deciderSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: oneSig.ID,
			Resources: []chnl.ID{
				follower.ID,
			},
		}
		decider, err := dealAPI.Involve(deciderSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		caseSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: follower.ID,
			Term: step.CaseSpec{
				X: follower.ID,
				Conts: map[core.Label]step.Term{
					label: step.CloseSpec{
						X: follower.ID,
					},
				},
			},
		}
		// when
		err = dealAPI.Take(caseSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		labSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: decider.ID,
			Term: step.LabSpec{
				X: follower.ID,
				L: label,
			},
		}
		// and
		err = dealAPI.Take(labSpec)
		if err != nil {
			t.Fatal(err)
		}
		// then
		// TODO добавить проверку
	})

	t.Run("Spawn", func(t *testing.T) {
		tc.Setup(t)
		// given
		oneRole, err := roleAPI.Create(
			role.Spec{
				FQN:   "one-role",
				State: state.OneSpec{},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSig1, err := sigAPI.Create(
			sig.Spec{
				FQN: "sig-1",
				PE: chnl.Spec{
					Key:  "chnl-1",
					Link: oneRole.FQN,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSig2, err := sigAPI.Create(
			sig.Spec{
				FQN: "sig-2",
				PE: chnl.Spec{
					Key:  "chnl-2",
					Link: oneRole.FQN,
				},
				CEs: []chnl.Spec{
					oneSig1.X2,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSig3, err := sigAPI.Create(
			sig.Spec{
				FQN: "sig-3",
				PE: chnl.Spec{
					Key:  "chnl-3",
					Link: oneRole.FQN,
				},
				CEs: []chnl.Spec{
					oneSig1.X2,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		bigDeal, err := dealAPI.Create(
			deal.Spec{
				Name: "deal-1",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		injectee, err := dealAPI.Involve(
			deal.PartSpec{
				Deal:    bigDeal.ID,
				Service: oneSig1.ID,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		spawnerSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: oneSig2.ID,
			Resources: []chnl.ID{
				injectee.ID,
			},
		}
		spawner, err := dealAPI.Involve(spawnerSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		z := sym.New("z")
		// and
		spawnSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: spawner.ID,
			Term: step.SpawnSpec{
				X: z,
				Ys2: []chnl.ID{
					injectee.ID,
				},
				Cont: step.WaitSpec{
					X: z,
					Cont: step.CloseSpec{
						X: spawner.ID,
					},
				},
				SigID: oneSig3.ID,
			},
		}
		// when
		err = dealAPI.Take(spawnSpec)
		if err != nil {
			t.Fatal(err)
		}
		// then
		// TODO добавить проверку
	})

	t.Run("Fwd", func(t *testing.T) {
		tc.Setup(t)
		// given
		oneRole, err := roleAPI.Create(
			role.Spec{
				FQN:   "one-role",
				State: state.OneSpec{},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSig1, err := sigAPI.Create(
			sig.Spec{
				FQN: "sig-1",
				PE: chnl.Spec{
					Key:  "chnl-1",
					Link: oneRole.FQN,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSig2, err := sigAPI.Create(
			sig.Spec{
				FQN: "sig-2",
				PE: chnl.Spec{
					Key:  "chnl-2",
					Link: oneRole.FQN,
				},
				CEs: []chnl.Spec{
					oneSig1.X2,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		oneSig3, err := sigAPI.Create(
			sig.Spec{
				FQN: "sig-3",
				PE: chnl.Spec{
					Key:  "chnl-3",
					Link: oneRole.FQN,
				},
				CEs: []chnl.Spec{
					oneSig1.X2,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		bigDeal, err := dealAPI.Create(
			deal.Spec{
				Name: "deal-1",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		// and
		closerSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: oneSig1.ID,
		}
		closer, err := dealAPI.Involve(closerSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		forwarderSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: oneSig2.ID,
			Resources: []chnl.ID{
				closer.ID,
			},
		}
		forwarder, err := dealAPI.Involve(forwarderSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		waiterSpec := deal.PartSpec{
			Deal:    bigDeal.ID,
			Service: oneSig3.ID,
			Resources: []chnl.ID{
				forwarder.ID,
			},
		}
		waiter, err := dealAPI.Involve(waiterSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		closeSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: closer.ID,
			Term: step.CloseSpec{
				X: closer.ID,
			},
		}
		err = dealAPI.Take(closeSpec)
		if err != nil {
			t.Fatal(err)
		}
		// when
		fwdSpec := deal.TranSpec{
			Deal: bigDeal.ID,
			// канал пересыльщика должен закрыться?
			ProcID: forwarder.ID,
			Term: step.FwdSpec{
				X: forwarder.ID,
				Y: closer.ID,
			},
		}
		err = dealAPI.Take(fwdSpec)
		if err != nil {
			t.Fatal(err)
		}
		// and
		waitSpec := deal.TranSpec{
			Deal:   bigDeal.ID,
			ProcID: waiter.ID,
			Term: step.WaitSpec{
				X: forwarder.ID,
				Cont: step.CloseSpec{
					X: waiter.ID,
				},
			},
		}
		err = dealAPI.Take(waitSpec)
		if err != nil {
			t.Fatal(err)
		}
		// then
		// TODO добавить проверку
	})
}

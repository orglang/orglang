package step

import (
	"os"
	"slices"
	"testing"

	"smecalculus/rolevod/lib/id"

	"smecalculus/rolevod/internal/chnl"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestCollectCtx(t *testing.T) {
	// given
	ce := id.New()
	// and
	term := SpawnSpec{Ys2: []chnl.ID{ce}, Cont: CloseSpec{}}
	// when
	actualCEs := CollectCtx(id.New(), term)
	// then
	if !slices.Contains(actualCEs, ce) {
		t.Errorf("unexpected ces: want %q in %v", ce, actualCEs)
	}
}

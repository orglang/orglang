package prog

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMsgFromText(t *testing.T) {
	tests := map[string]struct {
		text string
		spec SpecME
	}{
		"one pool type": {
			text: `
				pool type t1 { }
			`,
			spec: SpecME{PoolTypes: []*PoolType{{QN: "t1"}}},
		},
		"two pool types": {
			text: `
				pool type t1 { }
				pool type t2 { }
			`,
			spec: SpecME{PoolTypes: []*PoolType{{QN: "t1"}, {QN: "t2"}}},
		},
		"pool type and proc type": {
			text: `
				pool type t1 { }
				proc type t2 { }
			`,
			spec: SpecME{PoolTypes: []*PoolType{{QN: "t1"}}, ProcTypes: []*ProcType{{QN: "t2"}}},
		},
		"pool type with one exp": {
			text: `
				pool type t1 {
					up { }
				}
			`,
			spec: SpecME{PoolTypes: []*PoolType{
				{QN: "t1", Exp: &TypeExp{
					Up: &UpExp{},
				}},
			}},
		},
		"pool type with two exps": {
			text: `
				pool type t1 {
					up {
						with { }
					}
				}
			`,
			spec: SpecME{PoolTypes: []*PoolType{
				{QN: "t1", Exp: &TypeExp{
					Up: &UpExp{Cont: &TypeExp{
						With: &WithExp{},
					}},
				}},
			}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := msgFromText(test.text)
			if err != nil {
				t.Fatal(err)
			}
			if !cmp.Equal(got, test.spec) {
				// t.Logf("text:%s", test.text)
				t.Errorf("\ntext:%s\nspec:\n%s", test.text, cmp.Diff(got, test.spec))
			}
		})
	}
}

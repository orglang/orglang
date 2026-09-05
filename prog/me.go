package prog

import "fmt"

type SpecME struct {
	PoolTypes []*PoolType `@@*`
	ProcTypes []*ProcType `@@*`
}

type PoolType struct {
	QN  string   `"pool" "type" @Ident`
	Exp *TypeExp `"{" @@? "}"`
}

type TypeExp struct {
	// K Kind `(?= @("up"|"down"|"with"|"link") )`
	K Kind `@(?= "up"|"down"|"with"|"link" )`
	// K    Kind     `(?= "foo"|"bar" )`
	Up   *UpExp   `@@`
	Down *DownExp `| @@`
	With *WithExp `| @@`
	Link *LinkExp `| @@`
}

type Kind string

func (k *Kind) Capture(values []string) error {
	fmt.Printf("kind: %v", values)
	return nil
}

type UpExp struct {
	K    string   `"up"`
	Cont *TypeExp `"{" @@? "}"`
}

type DownExp struct {
	K    string   `"down"`
	Cont *TypeExp `"{" @@? "}"`
}

type WithExp struct {
	K    string   `"with"`
	QNs  []string `"(" @Ident* ")"`
	Cont *TypeExp `"{" @@? "}"`
}

type LinkExp struct {
	K  string `"link"`
	QN string `@Ident`
}

type ProcType struct {
	QN  string   `"proc" "type" @Ident`
	Exp *TypeExp `"{" @@? "}"`
}

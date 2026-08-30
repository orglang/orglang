package prog

type SpecME struct {
	PoolTypes []*PoolType `@@*`
	ProcTypes []*ProcType `@@*`
}

type PoolType struct {
	QN  string   `"pool" "type" @Ident`
	Exp *TypeExp `"{" @@? "}"`
}

type TypeExp struct {
	Up   *UpExp   `@@`
	With *WithExp `| @@`
}

type UpExp struct {
	Cont *TypeExp `"up" "{" @@? "}"`
}

type WithExp struct {
	K string `"with"`
	// QNs  []string `"(" @String+ ")"`
	Cont *TypeExp `"{" @@? "}"`
}

type ProcType struct {
	QN  string   `"proc" "type" @Ident`
	Exp *TypeExp `"{" @@? "}"`
}

package pol

type ADT int8

const (
	// provider -> client
	Pos  = ADT(+1)
	Zero = ADT(0)
	// client -> provider
	Neg = ADT(-1)
)

type Polarizable interface {
	Pol() ADT
}

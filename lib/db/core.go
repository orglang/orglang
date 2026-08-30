package db

import (
	"context"
	"fmt"

	"go.uber.org/fx"
)

// Unit of work
type UoW struct {
	Ctx context.Context
	Pgx driverPgx
}

type Transactor interface {
	ExplicitTx(context.Context, func(UoW) error) error
	ImplicitTx(context.Context, func(UoW) error) error
}

func newTransactor(props storageProps, lc fx.Lifecycle) (Transactor, error) {
	switch {
	case props.Protocol.Mode == postgresMode && props.Driver.Mode == pgxMode:
		pool, err := newPoolPgx(props, lc)
		if err != nil {
			return nil, err
		}
		return newTransactorPgx(pool), nil
	default:
		panic(fmt.Sprintf("unsupported combination: %s & %s", props.Protocol.Mode, props.Driver.Mode))
	}
}

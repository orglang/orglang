package db

import (
	"context"
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

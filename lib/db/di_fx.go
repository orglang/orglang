package db

import (
	"go.uber.org/fx"
)

var Module = fx.Module("lib/db",
	fx.Provide(
		newPoolPgx,
		fx.Annotate(newTransactorPgx, fx.As(new(Transactor))),
	),
	fx.Provide(
		fx.Private,
		newStorageCS,
	),
)

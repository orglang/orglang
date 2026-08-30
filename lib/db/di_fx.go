package db

import (
	"go.uber.org/fx"
)

var Module = fx.Module("lib/db",
	fx.Provide(
		newTransactor,
	),
	fx.Provide(
		fx.Private,
		newStorageProps,
	),
)

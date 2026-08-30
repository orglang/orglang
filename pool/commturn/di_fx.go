package commturn

import (
	"go.uber.org/fx"
)

var Module = fx.Module("pool/commturn",
	fx.Provide(
		fx.Annotate(newService, fx.As(new(API))),
		fx.Annotate(newDaoPgx, fx.As(new(Repo))),
	),
	fx.Provide(
		fx.Private,
		newControllerEcho,
		fx.Annotate(newSQLBuilder, fx.As(new(queryBuilder))),
	),
	fx.Invoke(
		cfgEchoController,
	),
)

package termdef

import (
	"go.uber.org/fx"

	"orglang/go-engine/adt/descsem"
)

var Module = fx.Module("pool/termdef",
	fx.Provide(
		fx.Annotate(newService, fx.As(new(API))),
		fx.Annotate(newDaoPgx, fx.As(new(Repo))),
	),
	fx.Provide(
		fx.Private,
		newControllerEcho,
		fx.Annotate(newSQLBuilder, fx.As(new(queryBuilder))),
		fx.Annotate(descsem.NewDaoPgx(descBinds), fx.As(new(descsem.Repo))),
	),
	fx.Invoke(
		cfgEchoController,
	),
)

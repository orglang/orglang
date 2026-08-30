package termdec

import (
	"go.uber.org/fx"

	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/lib/te"
)

var Module = fx.Module("proc/termdec",
	fx.Provide(
		fx.Annotate(newService, fx.As(new(API))),
		fx.Annotate(newDaoPgx, fx.As(new(Repo))),
	),
	fx.Provide(
		fx.Private,
		newControllerEcho,
		newEchoPresenter,
		fx.Annotate(newSQLBuilder, fx.As(new(queryBuilder))),
		fx.Annotate(newRendererStdlib, fx.As(new(te.Renderer))),
		fx.Annotate(typesem.NewDaoPgx(typeDefs, descBinds), fx.As(new(typesem.Repo))),
	),
	fx.Invoke(
		cfgEchoController,
		cfgEchoPresenter,
	),
)

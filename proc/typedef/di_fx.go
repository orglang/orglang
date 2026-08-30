package typedef

import (
	"go.uber.org/fx"

	"orglang/go-engine/lib/te"

	"orglang/go-engine/adt/descsem"
)

var Module = fx.Module("adproct/typedef",
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
		fx.Annotate(descsem.NewDaoPgx(descBinds), fx.As(new(descsem.Repo))),
	),
	fx.Invoke(
		cfgEchoController,
		cfgEchoPresenter,
	),
)

package compexec

import (
	"go.uber.org/fx"

	"orglang/go-engine/adt/compsem"
	"orglang/go-engine/adt/implsem"
)

var Module = fx.Module("pool/compexec",
	fx.Provide(
		fx.Annotate(newService, fx.As(new(API))),
		fx.Annotate(newDaoPgx, fx.As(new(Repo))),
	),
	fx.Provide(
		fx.Private,
		newControllerEcho,
		newPondBroker,
		fx.Annotate(newPondBroker, fx.As(new(Broker))),
		// fx.Annotate(newWorkerPoolBroker, fx.As(new(Exch))),
		fx.Annotate(newSQLBuilder, fx.As(new(queryBuilder))),
		fx.Annotate(implsem.NewDaoPgx(implBinds), fx.As(new(implsem.Repo))),
		fx.Annotate(compsem.NewDaoPgx(compExecs), fx.As(new(compsem.Repo))),
	),
	fx.Invoke(
		cfgEchoController,
		cfgPondBroker,
		// fx.Annotate(cfgPondBroker, fx.From(new(Exch))),
		// cfgWorkerPoolBroker,
	),
)

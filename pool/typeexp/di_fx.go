package typeexp

import (
	"go.uber.org/fx"
)

var Module = fx.Module("pool/typeexp",
	fx.Provide(
		fx.Annotate(newDaoPgx, fx.As(new(Repo))),
	),
	fx.Provide(
		fx.Private,
		fx.Annotate(newSQLBuilder, fx.As(new(queryBuilder))),
	),
)

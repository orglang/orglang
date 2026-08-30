package commexch

import (
	"go.uber.org/fx"
)

var Module = fx.Module("proc/commexch",
	fx.Provide(
		fx.Annotate(newDaoPgx, fx.As(new(Repo))),
	),
)

package termexp

import (
	"go.uber.org/fx"
)

var Module = fx.Module("proc/termexp",
	fx.Provide(
		fx.Annotate(newDaoPgx, fx.As(new(Repo))),
	),
)

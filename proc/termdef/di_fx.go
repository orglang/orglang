package termdef

import (
	"go.uber.org/fx"
)

var Module = fx.Module("proc/termdef",
	fx.Provide(
		fx.Annotate(newDaoPgx, fx.As(new(Repo))),
	),
)

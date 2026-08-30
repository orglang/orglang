package prog

import (
	"log/slog"
	"reflect"

	"github.com/labstack/echo/v4"
)

type API interface {
	Create(spec Spec) error
}

// Server-side primary adapter
type controllerEcho struct {
	api API
	log *slog.Logger
}

func newControllerEcho(a API, l *slog.Logger) *controllerEcho {
	name := slog.String("name", reflect.TypeFor[controllerEcho]().Name())
	return &controllerEcho{a, l.With(name)}
}

func cfgControllerEcho(e *echo.Echo, c *controllerEcho) error {
	e.POST("/api/v1/progs", c.PostSpec)
	return nil
}

func (c *controllerEcho) PostSpec(ctx echo.Context) error {
	return nil
}

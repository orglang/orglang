package commturn

import (
	"log/slog"
	"reflect"

	"github.com/labstack/echo/v4"
)

// Server-side primary adapter
type controllerEcho struct {
	api API
	log *slog.Logger
}

func newControllerEcho(api API, log *slog.Logger) *controllerEcho {
	name := slog.String("name", reflect.TypeFor[controllerEcho]().Name())
	return &controllerEcho{api, log.With(name)}
}

func cfgEchoController(e *echo.Echo, h *controllerEcho) error {
	return nil
}

package web

import (
	"log/slog"
	"net/http"
	"reflect"

	"github.com/labstack/echo/v4"

	"orglang/go-engine/lib/te"

	"orglang/go-engine/adt/typesem"
	"orglang/go-engine/proc/typedef"
)

// Adapter
type controllerEcho struct {
	api typedef.API
	ssr te.Renderer
	log *slog.Logger
}

func newControllerEcho(a typedef.API, r te.Renderer, l *slog.Logger) *controllerEcho {
	name := slog.String("name", reflect.TypeFor[controllerEcho]().Name())
	return &controllerEcho{a, r, l.With(name)}
}

func cfgEchoController(e *echo.Echo, h *controllerEcho) {
	e.GET("/", h.Home)
}

func (h *controllerEcho) Home(c echo.Context) error {
	refs, err := h.api.RetreiveRefs()
	if err != nil {
		return err
	}
	html, err := h.ssr.Render("home.html", typesem.MsgFromRefs(refs))
	if err != nil {
		return err
	}
	return c.HTMLBlob(http.StatusOK, html)
}

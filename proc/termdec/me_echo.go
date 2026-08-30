package termdec

import (
	"log/slog"
	"net/http"
	"reflect"

	"github.com/labstack/echo/v4"

	sdk "github.com/orglang/go-sdk/adt/termsem"
	"github.com/orglang/go-sdk/proc/termdec"

	"orglang/go-engine/adt/termsem"
)

// Server-side primary adapter
type controllerEcho struct {
	api API
	log *slog.Logger
}

func newControllerEcho(a API, l *slog.Logger) *controllerEcho {
	name := slog.String("name", reflect.TypeFor[controllerEcho]().Name())
	return &controllerEcho{a, l.With(name)}
}

func cfgEchoController(e *echo.Echo, h *controllerEcho) error {
	e.POST("/api/v1/procs/decs", h.PostSpec)
	e.GET("/api/v1/procs/decs/:id", h.GetSnap)
	return nil
}

func (h *controllerEcho) PostSpec(c echo.Context) error {
	var dto termdec.DecSpec
	bindErr := c.Bind(&dto)
	if bindErr != nil {
		h.log.Error("binding failed", slog.Any("dto", reflect.TypeOf(dto)))
		return bindErr
	}
	validateErr := dto.Validate()
	if validateErr != nil {
		h.log.Error("validation failed", slog.Any("dto", dto))
		return validateErr
	}
	spec, convErr := MsgToDecSpec(dto)
	if convErr != nil {
		h.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	snap, createErr := h.api.Create(spec)
	if createErr != nil {
		return createErr
	}
	return c.JSON(http.StatusCreated, MsgFromDecSnap(snap))
}

func (h *controllerEcho) GetSnap(c echo.Context) error {
	var dto sdk.SemRef
	bindErr := c.Bind(&dto)
	if bindErr != nil {
		h.log.Error("binding failed", slog.Any("dto", reflect.TypeOf(dto)))
		return bindErr
	}
	ref, convErr := termsem.MsgToRef(dto)
	if convErr != nil {
		h.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	snap, retrieveErr := h.api.RetrieveSnap(ref)
	if retrieveErr != nil {
		return retrieveErr
	}
	return c.JSON(http.StatusOK, MsgFromDecSnap(snap))
}

package compexec

import (
	"log/slog"
	"net/http"
	"reflect"

	"github.com/labstack/echo/v4"

	sdk1 "github.com/orglang/go-sdk/adt/compsem"
	sdk2 "github.com/orglang/go-sdk/proc/compstep"

	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/compsem"
	"orglang/go-engine/proc/compstep"
)

// Server-side primary adapter
type controllerEcho struct {
	api API
	log *slog.Logger
}

func newControllerEcho(a API, l *slog.Logger) *controllerEcho {
	return &controllerEcho{a, l}
}

func cfgEchoController(e *echo.Echo, h *controllerEcho) error {
	e.GET("/api/v1/procs/:id", h.GetSnap)
	e.POST("/api/v1/procs/:id/steps", h.PostStep)
	return nil
}

func (h *controllerEcho) GetSnap(c echo.Context) error {
	var dto sdk1.SemRef
	bindErr := c.Bind(&dto)
	if bindErr != nil {
		h.log.Error("binding failed", slog.Any("dto", dto))
		return bindErr
	}
	ref, convErr := compsem.MsgToRef(dto)
	if convErr != nil {
		h.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	snap, retrieveErr := h.api.RetrieveSnap(ref)
	if retrieveErr != nil {
		return retrieveErr
	}
	return c.JSON(http.StatusOK, MsgFromExecSnap(snap))
}

func (h *controllerEcho) PostStep(c echo.Context) error {
	var dto sdk2.StepSpec
	bindErr := c.Bind(&dto)
	if bindErr != nil {
		h.log.Error("binding failed", slog.Any("dto", reflect.TypeOf(dto)))
		return bindErr
	}
	ctx := c.Request().Context()
	h.log.Log(ctx, lf.LevelTrace, "posting started", slog.Any("dto", dto))
	validateErr := dto.Validate()
	if validateErr != nil {
		h.log.Error("validation failed", slog.Any("dto", dto))
		return validateErr
	}
	spec, convErr := compstep.MsgToStepSpec(dto)
	if convErr != nil {
		h.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	takingErr := h.api.Take(spec)
	if takingErr != nil {
		return takingErr
	}
	return c.NoContent(http.StatusOK)
}

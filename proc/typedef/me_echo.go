package typedef

import (
	"log/slog"
	"net/http"
	"reflect"

	"github.com/labstack/echo/v4"

	sdk "github.com/orglang/go-sdk/adt/typesem"
	"github.com/orglang/go-sdk/proc/typedef"

	"orglang/go-engine/lib/lf"

	"orglang/go-engine/adt/typesem"
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
	e.POST("/api/v1/types", h.PostSpec)
	e.GET("/api/v1/types/:id", h.GetSnap)
	e.PATCH("/api/v1/types/:id", h.PatchOne)
	return nil
}

func (h *controllerEcho) PostSpec(c echo.Context) error {
	var dto typedef.DefSpec
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
	spec, convErr := MsgToDefSpec(dto)
	if convErr != nil {
		h.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	snap, createErr := h.api.Create(spec)
	if createErr != nil {
		return createErr
	}
	h.log.Log(ctx, lf.LevelTrace, "posting succeed", slog.Any("ref", snap.TypeRef))
	return c.JSON(http.StatusCreated, MsgFromDefSnap(snap))
}

func (h *controllerEcho) GetSnap(c echo.Context) error {
	var dto sdk.SemRef
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
	ref, convErr := typesem.MsgToRef(dto)
	if convErr != nil {
		h.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	snap, retrieveErr := h.api.RetrieveSnap(ref)
	if retrieveErr != nil {
		return retrieveErr
	}
	return c.JSON(http.StatusOK, MsgFromDefSnap(snap))
}

func (h *controllerEcho) PatchOne(c echo.Context) error {
	var dto typedef.DefSnap
	bindErr := c.Bind(&dto)
	if bindErr != nil {
		h.log.Error("binding failed", slog.Any("dto", reflect.TypeOf(dto)))
		return bindErr
	}
	ctx := c.Request().Context()
	h.log.Log(ctx, lf.LevelTrace, "patching started", slog.Any("dto", dto))
	validateErr := dto.Validate()
	if validateErr != nil {
		h.log.Error("validation failed", slog.Any("dto", dto))
		return validateErr
	}
	reqSnap, convErr := MsgToDefSnap(dto)
	if convErr != nil {
		h.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	resSnap, modificationErr := h.api.Modify(reqSnap)
	if modificationErr != nil {
		return modificationErr
	}
	h.log.Log(ctx, lf.LevelTrace, "patching succeed", slog.Any("ref", resSnap.TypeRef))
	return c.JSON(http.StatusOK, MsgFromDefSnap(resSnap))
}

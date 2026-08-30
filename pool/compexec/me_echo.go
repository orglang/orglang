package compexec

import (
	"log/slog"
	"net/http"
	"reflect"

	"github.com/labstack/echo/v4"

	"github.com/orglang/go-sdk/pool/compexec"
	sdk "github.com/orglang/go-sdk/pool/compstep"

	"orglang/go-engine/adt/compsem"
	"orglang/go-engine/pool/compstep"
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

func cfgEchoController(server *echo.Echo, controller *controllerEcho) error {
	server.POST("/api/v1/pools/execs", controller.PostSpec)
	server.POST("/api/v1/pools/execs/steps", controller.PostSpec2)
	server.POST("/api/v1/pools/execs/spawns", controller.PostSpec3)
	return nil
}

func (c *controllerEcho) PostSpec(ctx echo.Context) error {
	var dto compexec.ExecSpec
	bindErr := ctx.Bind(&dto)
	if bindErr != nil {
		c.log.Error("binding failed", slog.Any("dto", reflect.TypeOf(dto)))
		return bindErr
	}
	validErr := dto.Validate()
	if validErr != nil {
		c.log.Error("validation failed", slog.Any("dto", dto))
		return validErr
	}
	spec, convErr := MsgToExecSpec(dto)
	if convErr != nil {
		c.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	ref, apiErr := c.api.Run(spec)
	if apiErr != nil {
		return apiErr
	}
	return ctx.JSON(http.StatusCreated, compsem.MsgFromRef(ref))
}

func (c *controllerEcho) PostSpec2(ctx echo.Context) error {
	var dto sdk.StepSpec
	bindErr := ctx.Bind(&dto)
	if bindErr != nil {
		c.log.Error("binding failed", slog.Any("dto", reflect.TypeFor[sdk.StepSpec]()))
		return bindErr
	}
	validErr := dto.Validate()
	if validErr != nil {
		c.log.Error("validation failed", slog.Any("dto", dto))
		return validErr
	}
	spec, convErr := compstep.MsgToStepSpec(dto)
	if convErr != nil {
		c.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	apiErr := c.api.Take(spec)
	if apiErr != nil {
		return apiErr
	}
	return ctx.NoContent(http.StatusNoContent)
}

func (c *controllerEcho) PostSpec3(ctx echo.Context) error {
	var dto sdk.StepSpec
	bindErr := ctx.Bind(&dto)
	if bindErr != nil {
		c.log.Error("binding failed", slog.Any("dto", reflect.TypeFor[sdk.StepSpec]()))
		return bindErr
	}
	validateErr := dto.Validate()
	if validateErr != nil {
		c.log.Error("validation failed", slog.Any("dto", dto))
		return validateErr
	}
	spec, convErr := compstep.MsgToStepSpec(dto)
	if convErr != nil {
		c.log.Error("conversion failed", slog.Any("dto", dto))
		return convErr
	}
	ref, apiErr := c.api.Spawn(spec)
	if apiErr != nil {
		return apiErr
	}
	return ctx.JSON(http.StatusCreated, compsem.MsgFromRef(ref))
}

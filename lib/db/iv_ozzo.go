package db

import (
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

func (dto storageProps) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.Protocol, validation.Required),
		validation.Field(&dto.Driver, validation.Required),
	)
}

func (dto protoProps) Validate() error {
	err := validation.ErrInInvalid.SetMessage(enumValueMessage).SetParams(map[string]any{
		"value": dto.Mode, "enum": []protoMode{postgresMode},
	})
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.Mode, validation.Required, validation.In(postgresMode).ErrorObject(err)),
		validation.Field(&dto.Postgres, validation.Required.When(dto.Mode == postgresMode)),
	)
}

func (dto postgresProps) Validate() error {
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.URL, validation.Required),
	)
}

func (dto driverProps) Validate() error {
	err := validation.ErrInInvalid.SetMessage(enumValueMessage).SetParams(map[string]any{
		"value": dto.Mode, "enum": []driverMode{pgxMode},
	})
	return validation.ValidateStruct(&dto,
		validation.Field(&dto.Mode, validation.Required, validation.In(pgxMode).ErrorObject(err)),
		validation.Field(&dto.Pgx, validation.Required.When(dto.Mode == pgxMode)),
	)
}

func (dto pgxProps) Validate() error {
	return validation.ValidateStruct(&dto)
}

const (
	enumValueMessage = "got '{{.value}}', want {{.enum}}"
)

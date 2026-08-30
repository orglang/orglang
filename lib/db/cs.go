package db

import (
	"orglang/go-engine/lib/kv"
)

func newStorageProps(l kv.Loader) (storageProps, error) {
	dto := new(storageProps)
	loadErr := l.Load("storage", dto)
	if loadErr != nil {
		return storageProps{}, loadErr
	}
	validateErr := dto.Validate()
	if validateErr != nil {
		return storageProps{}, validateErr
	}
	return *dto, nil
}

type storageProps struct {
	Protocol protoProps  `mapstructure:"protocol"`
	Driver   driverProps `mapstructure:"driver"`
}

type protoProps struct {
	Mode     protoMode     `mapstructure:"mode"`
	Postgres postgresProps `mapstructure:"postgres"`
}

type driverProps struct {
	Mode driverMode `mapstructure:"mode"`
	Pgx  pgxProps   `mapstructure:"pgx"`
}

type postgresProps struct {
	URL string `mapstructure:"url"`
}

type pgxProps struct {
	MaxConns uint16 `mapstructure:"max_conns"`
}

type protoMode string

const (
	postgresMode protoMode = "postgres"
)

type driverMode string

const (
	pgxMode driverMode = "pgx"
)

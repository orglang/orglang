package db

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

type driverPgx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

func newTransactorPgx(pool *pgxpool.Pool) *TransactorPgx {
	return &TransactorPgx{pool}
}

func newPoolPgx(dto storageCS, lc fx.Lifecycle) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dto.Protocol.Postgres.URL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = int32(dto.Driver.Pgx.MaxConns)
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}
	lc.Append(
		fx.Hook{
			OnStart: pool.Ping,
			OnStop: func(ctx context.Context) error {
				go pool.Close()
				return nil
			},
		},
	)
	return pool, nil
}

type TransactorPgx struct {
	pool *pgxpool.Pool
}

func (t *TransactorPgx) ExplicitTx(ctx context.Context, fn func(UoW) error) error {
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return err
	}
	err = fn(UoW{Ctx: ctx, Pgx: tx})
	if err != nil {
		return errors.Join(err, tx.Rollback(ctx))
	}
	return tx.Commit(ctx)
}

func (t *TransactorPgx) ImplicitTx(ctx context.Context, fn func(UoW) error) error {
	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return fn(UoW{Ctx: ctx, Pgx: conn.Conn()})
}

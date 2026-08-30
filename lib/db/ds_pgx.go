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

func newTransactorPgx(pool *pgxpool.Pool) *transactorPgx {
	return &transactorPgx{pool}
}

func newPoolPgx(props storageProps, lc fx.Lifecycle) (*pgxpool.Pool, error) {
	config, parseErr := pgxpool.ParseConfig(props.Protocol.Postgres.URL)
	if parseErr != nil {
		return nil, parseErr
	}
	config.MaxConns = int32(props.Driver.Pgx.MaxConns)
	pool, createErr := pgxpool.NewWithConfig(context.Background(), config)
	if createErr != nil {
		return nil, createErr
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

type transactorPgx struct {
	pool *pgxpool.Pool
}

func (t *transactorPgx) ExplicitTx(ctx context.Context, fn func(UoW) error) error {
	tx, beginErr := t.pool.Begin(ctx)
	if beginErr != nil {
		return beginErr
	}
	invokeErr := fn(UoW{Ctx: ctx, Pgx: tx})
	if invokeErr != nil {
		return errors.Join(invokeErr, tx.Rollback(ctx))
	}
	return tx.Commit(ctx)
}

func (t *transactorPgx) ImplicitTx(ctx context.Context, fn func(UoW) error) error {
	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return fn(UoW{Ctx: ctx, Pgx: conn})
}

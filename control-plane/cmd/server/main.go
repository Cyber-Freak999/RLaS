package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"rlas/control-plane/internal"
	"rlas/redis" // package redisclient
)

// redisPinger adapts the go-redis client (whose Ping returns *StatusCmd) to
// the internal.Pinger interface used by /healthz.
type redisPinger struct{ rdb *redis.Client }

func (p redisPinger) Ping(ctx context.Context) error {
	return p.rdb.Ping(ctx).Err()
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg := internal.LoadConfig()
	if err := cfg.Validate(); err != nil {
		logger.Error("config_invalid", "error", err.Error())
		os.Exit(1)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	rdb := redisclient.NewFailoverClient(redisclient.FailoverConfig{
		MasterName:    cfg.Redis.MasterName,
		SentinelAddrs: cfg.Redis.SentinelAddrs,
		Password:      cfg.Redis.Password,
	})
	defer rdb.Close()

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	pool, err := pgxpool.New(startupCtx, cfg.TimescaleDSN)
	if err != nil {
		logger.Error("db_connect_failed", "error", err.Error())
		os.Exit(1)
	}
	defer pool.Close()
	if err := internal.CreateSchema(startupCtx, pool); err != nil {
		logger.Error("db_schema_failed", "error", err.Error())
		os.Exit(1)
	}
	startupCancel()

	store := internal.NewPGStore(pool)
	admin := internal.NewAdmin(rdb, store, redisPinger{rdb}, store, cfg.AdminToken, logger)
	consumer := internal.NewConsumer(rdb, store, logger, internal.ConsumerOptions{})

	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.Run(consumerCtx)
	}()

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(cfg.Port),
		Handler: admin.Handler(),
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	logger.Info("control_plane_started", "port", cfg.Port)

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server_error", "error", err.Error())
		}
	case <-rootCtx.Done():
		logger.Info("shutdown_signal_received")
	}

	// Graceful shutdown (constraint 12): stop the consumer's blocking read,
	// then drain in-flight HTTP requests before exiting.
	consumerCancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("shutdown_error", "error", err.Error())
	}
	wg.Wait()
	logger.Info("control_plane_stopped")
}

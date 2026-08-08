package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"rlas/rate-limiter/internal"
	"rlas/redis"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := internal.LoadConfig()

	client, err := redisclient.NewClient(cfg.Redis)
	if err != nil {
		logger.Error("redis_config_invalid", "error", err.Error())
		os.Exit(1)
	}
	defer client.Close()

	limiter := internal.NewLimiter(internal.LimiterOptions{
		Redis:        client,
		Checker:      redisclient.NewChecker(client),
		Logger:       logger,
		CheckTimeout: cfg.CheckTimeout,
	})

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           internal.Server(limiter),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Graceful shutdown: drain in-flight requests on SIGTERM/SIGINT so the
	// chaos test sees a clean stop, not dropped connections (constraint 12).
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		logger.Info("http_listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http_listen_failed", "error", err.Error())
			os.Exit(1)
		}
	}()

	sig := <-stop
	logger.Info("shutdown_signal", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown_error", "error", err.Error())
	}
	logger.Info("shutdown_complete")
}

// api — дашборд Sentinel: список инцидентов из MongoDB, живое
// обновление через SSE. Один бинарник: и API, и встроенный фронтенд.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/WOLFraMix/sentinel/internal/api"
	"github.com/WOLFraMix/sentinel/internal/config"
	"github.com/WOLFraMix/sentinel/internal/env"
	"github.com/WOLFraMix/sentinel/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelInfo})))
	log := slog.Default()
	env.Load(".env")
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := storage.Connect(ctx, cfg.MongoURL, cfg.MongoDB)
	if err != nil {
		log.Error("mongo", "err", err)
		os.Exit(1)
	}
	defer st.Close(context.Background())

	log.Info("dashboard listening", "addr", cfg.APIAddr)
	if err := api.New(st, log).Run(ctx, cfg.APIAddr); err != nil {
		log.Error("api", "err", err)
		os.Exit(1)
	}
}

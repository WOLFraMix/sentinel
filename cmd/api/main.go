// api — дашборд Sentinel: список инцидентов из MongoDB, живое
// обновление через SSE. Один бинарник: и API, и встроенный фронтенд.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/WOLFraMix/sentinel/internal/api"
	"github.com/WOLFraMix/sentinel/internal/config"
	"github.com/WOLFraMix/sentinel/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelInfo})))
	log := slog.Default()
	loadEnvFile()
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

// loadEnvFile — тот же мини-парсер; на итерации Docker Compose
// вынесем его в internal/env и уберём дублирование из трёх main.
func loadEnvFile() {
	if _, err := os.Stat(".env"); err != nil {
		return
	}
	data, _ := os.ReadFile(".env")
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			if os.Getenv(strings.TrimSpace(k)) == "" {
				_ = os.Setenv(strings.TrimSpace(k), strings.Trim(strings.TrimSpace(v), `"'`))
			}
		}
	}
}

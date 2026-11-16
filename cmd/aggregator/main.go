// aggregator — потребитель Sentinel: читает события из JetStream,
// дедуплицирует по ID и ведёт инциденты в MongoDB.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/WOLFraMix/sentinel/internal/config"
	"github.com/WOLFraMix/sentinel/internal/consumer"
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
	defer func() {
		if err := st.Close(context.Background()); err != nil {
			log.Error("mongo close", "err", err)
		}
	}()
	log.Info("mongo connected", "db", cfg.MongoDB)

	js, err := consumer.Connect(cfg.NATSURL)
	if err != nil {
		log.Error("nats", "err", err)
		os.Exit(1)
	}

	run := consumer.New(js, st, log)
	if err := run.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("aggregator stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("aggregator stopped gracefully")
}

// loadEnvFile — пока дублируем парсер .env (это 15 строк). Когда
// появится третий компонент с общим кодом, вынесем в internal/config.
func loadEnvFile() {
	if _, err := os.Stat(".env"); err != nil {
		return
	}
	data, err := os.ReadFile(".env")
	if err != nil {
		return
	}
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

// notifier — Sentinel-коннектор Jira: четвёртый независимый
// потребитель потока incidents. При превышении порога создаёт
// тикет (один на инцидент), повторы уходят комментариями.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/WOLFraMix/sentinel/internal/config"
	"github.com/WOLFraMix/sentinel/internal/events"
	"github.com/WOLFraMix/sentinel/internal/jira"
	"github.com/WOLFraMix/sentinel/internal/publisher"
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

	jc := jira.New(cfg.JiraURL, cfg.JiraEmail, cfg.JiraToken)
	log.Info("jira ready", "url", cfg.JiraURL, "project", cfg.JiraProject,
		"threshold", cfg.AlertThreshold, "window", cfg.AlertWindow)

	nc, err := nats.Connect(cfg.NATSURL,
		nats.Timeout(10*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1))
	if err != nil {
		log.Error("nats", "err", err)
		os.Exit(1)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("jetstream", "err", err)
		os.Exit(1)
	}

	n := &notifier{st: st, jira: jc, cfg: cfg, log: log,
		lastComment: map[string]time.Time{}}

	cc, err := js.CreateOrUpdateConsumer(ctx, publisher.StreamName,
		jetstream.ConsumerConfig{
			Name:          "notifier",
			FilterSubject: publisher.SubjectAll,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
		})
	if err != nil {
		log.Error("consumer", "err", err)
		os.Exit(1)
	}

	conCtx, err := cc.Consume(n.handle(ctx))
	if err != nil {
		log.Error("consume", "err", err)
		os.Exit(1)
	}

	<-ctx.Done()
	conCtx.Drain()
	select {
	case <-conCtx.Closed():
	case <-time.After(5 * time.Second):
	}
	log.Info("notifier stopped gracefully")
}

type notifier struct {
	st          *storage.Storage
	jira        *jira.Client
	cfg         config.Config
	log         *slog.Logger
	mu          sync.Mutex
	lastComment map[string]time.Time // защита от спама комментариями
}

func (n *notifier) handle(ctx context.Context) func(jetstream.Msg) {
	return func(msg jetstream.Msg) {
		var e events.Event
		if err := json.Unmarshal(msg.Data(), &e); err != nil {
			n.log.Error("bad json", "err", err)
			_ = msg.Term()
			return
		}

		incKey := fmt.Sprintf("%s/%s/%s/%s/%s",
			e.Cluster, e.Namespace, e.Pod, e.Container, e.Kind)

		inc, err := n.st.GetIncident(ctx, incKey)
		if err != nil {
			n.log.Error("get incident", "err", err)
			_ = msg.Nak()
			return
		}
		if inc == nil {
			// Aggregator ещё не записал инцидент (он обрабатывает тот же
			// поток параллельно). Вернём сообщение в очередь на 2 секунды.
			_ = msg.NakWithDelay(2 * time.Second)
			return
		}

		// Порог: count и свежий last_seen. Считаем только горячие
		// инциденты — вчерашний хвост не должен заводить тикеты.
		hot := time.Since(inc.LastSeen) <= n.cfg.AlertWindow
		if !hot || inc.Count < int64(n.cfg.AlertThreshold) {
			_ = msg.Ack()
			return
		}

		if inc.JiraKey != "" {
			n.commentThrottled(ctx, inc, &e)
			_ = msg.Ack()
			return
		}

		summary := fmt.Sprintf("[Sentinel] %s: %s/%s (restarts=%d, count=%d)",
			e.Kind, e.Namespace, e.Pod, e.Restarts, inc.Count)
		desc := fmt.Sprintf(
			"Инцидент: %s\nКластер: %s\nПод: %s/%s\nКонтейнер: %s\n"+
				"Причина: %s\nРестартов: %d\nСобытий: %d\n"+
				"Первое: %s\nПоследнее: %s",
			incKey, e.Cluster, e.Namespace, e.Pod, e.Container,
			e.Reason, e.Restarts, inc.Count,
			inc.FirstSeen.Format(time.RFC1123), inc.LastSeen.Format(time.RFC1123))

		ticket, err := n.jira.CreateIssue(ctx, n.cfg.JiraProject,
			n.cfg.JiraIssueType, summary, desc)
		if err != nil {
			// Ошибка Jira не должна терять событие — вернём в очередь.
			n.log.Error("jira create", "err", err)
			_ = msg.Nak()
			return
		}
		if err := n.st.SetJiraKey(ctx, incKey, ticket); err != nil {
			n.log.Error("save jira_key", "err", err)
			_ = msg.Nak()
			return
		}
		n.log.Info("jira ticket created", "ticket", ticket, "incident", incKey)
		_ = msg.Ack()
	}
}

// commentThrottled — не чаще одного комментария в 10 минут на инцидент.
func (n *notifier) commentThrottled(ctx context.Context, inc *storage.Incident, e *events.Event) {
	n.mu.Lock()
	last, seen := n.lastComment[inc.Key]
	n.mu.Unlock()
	if seen && time.Since(last) < 10*time.Minute {
		return
	}

	text := fmt.Sprintf("Повтор: %s, рестартов=%d, всего событий=%d (последнее %s)",
		e.Reason, e.Restarts, inc.Count, e.OccurredAt.Format(time.RFC1123))
	if err := n.jira.AddComment(ctx, inc.JiraKey, text); err != nil {
		n.log.Error("jira comment", "err", err)
		return
	}
	n.mu.Lock()
	n.lastComment[inc.Key] = time.Now()
	n.mu.Unlock()
	n.log.Info("jira commented", "ticket", inc.JiraKey, "pod", e.Namespace+"/"+e.Pod)
}

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

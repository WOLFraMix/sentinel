// Package consumer — чтение потока JetStream aggregator'ом.
//
// Стратегия подтверждений — at-least-once с честной обработкой:
//   - успешная обработка → Ack (сообщение удалено из неподтверждённых);
//   - битый JSON → Term (такое не починится повтором, терминально);
//   - ошибка базы → Nak (вернуть в очередь, повторить позже).
//
// Дедуп на стороне базы делает повторную доставку безопасной.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/WOLFraMix/sentinel/internal/events"
	"github.com/WOLFraMix/sentinel/internal/publisher"
	"github.com/WOLFraMix/sentinel/internal/storage"
)

type Consumer struct {
	js  jetstream.JetStream
	st  *storage.Storage
	log *slog.Logger
}

// Connect — клиент JetStream без создания потока: поток уже создал
// watcher; aggregator — только потребитель.
func Connect(url string) (jetstream.JetStream, error) {
	nc, err := nats.Connect(url,
		nats.Timeout(10*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return jetstream.New(nc)
}

func New(js jetstream.JetStream, st *storage.Storage, log *slog.Logger) *Consumer {
	return &Consumer{js: js, st: st, log: log}
}

// Run — durable-потребитель с именем "aggregator": офсет хранится
// на сервере, перезапуск продолжает с последнего подтверждённого
// сообщения, а не с начала потока.
func (c *Consumer) Run(ctx context.Context) error {
	cc, err := c.js.CreateOrUpdateConsumer(ctx, publisher.StreamName,
		jetstream.ConsumerConfig{
			Name:          "aggregator",
			FilterSubject: publisher.SubjectAll,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second, // сколько ждём ack перед повтором
			MaxDeliver:    10,               // потолок повторов битых сообщений
		})
	if err != nil {
		return fmt.Errorf("consumer: %w", err)
	}

	conCtx, err := cc.Consume(c.handle(ctx))
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	// Блокируемся до отмены контекста, потом останавливаем пул
	// обработчиков и даём им 5 секунд на завершение текущих сообщений.
	<-ctx.Done()
	conCtx.Stop()
	select {
	case <-conCtx.Done():
	case <-time.After(5 * time.Second):
	}
	return nil
}

func (c *Consumer) handle(ctx context.Context) func(jetstream.Msg) {
	return func(msg jetstream.Msg) {
		var e events.Event
		if err := json.Unmarshal(msg.Data(), &e); err != nil {
			// Битое сообщение повтором не лечится — терминально
			// выкидываем из очереди, но оставляем след в логе.
			c.log.Error("bad event json", "err", err, "subject", msg.Subject())
			_ = msg.Term()
			return
		}

		isNew, err := c.st.SaveEvent(ctx, e)
		if err != nil {
			// База недоступна — вернём сообщение в очередь: AckWait
			// истечёт, JetStream доставит снова. Событие не теряем.
			c.log.Error("save event", "err", err, "id", e.ID)
			_ = msg.Nak()
			return
		}
		if !isNew {
			_ = msg.Ack() // дубликат — уже в базе, ничего не делаем
			return
		}
		if err := c.st.BumpIncident(ctx, e); err != nil {
			c.log.Error("bump incident", "err", err, "id", e.ID)
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
		c.log.Info("incident recorded",
			"kind", e.Kind, "pod", e.Namespace+"/"+e.Pod,
			"reason", e.Reason, "restarts", e.Restarts)
	}
}

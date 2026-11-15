// Package publisher — публикация событий в NATS JetStream.
// JetStream, а не голый NATS: персистентность. Если aggregator лежит,
// события копятся в потоке и дождутся его — в этом смысл очереди:
// watcher работает независимо от потребителей.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/WOLFraMix/sentinel/internal/events"
)

const (
	StreamName = "incidents"
	SubjectAll = events.SubjectPrefix + ">" // все события потока
)

type Publisher struct {
	js jetstream.JetStream
}

// Connect подключается и создаёт/обновляет поток.
// Retention = Limits: события хранятся по времени (MaxAge), а не пока
// не прочитаны потребителем — история нужна и дашборду, не только
// aggregator'у.
func Connect(ctx context.Context, url string) (*Publisher, error) {
	// Reconnect-политика: брокер мог перезапуститься — клиент молча
	// восстановит соединение и продолжит публиковать.
	nc, err := nats.Connect(url,
		nats.Timeout(10*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1), // вечно ждать восстановления
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{SubjectAll},
		MaxAge:   7 * 24 * time.Hour, // неделя истории — стартовая политика
		MaxBytes: 512 << 20,          // 512 МБ потолок потока
	}); err != nil {
		return nil, fmt.Errorf("stream %s: %w", StreamName, err)
	}
	return &Publisher{js: js}, nil
}

// Publish публикует событие в тему incidents.<kind>. JetStream
// подтверждает приём сервером (Publish ждёт ack) — это и есть
// гарантия: событие либо в потоке, либо ошибка у вызвавшего.
func (p *Publisher) Publish(ctx context.Context, e events.Event) error {
	e.PublishedAt = time.Now().UTC()
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = p.js.Publish(ctx, events.SubjectPrefix+string(e.Kind), data)
	return err
}

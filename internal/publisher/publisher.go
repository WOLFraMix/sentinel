// Package publisher — публикация событий в NATS JetStream.
// JetStream, а не голый NATS: персистентность. Если aggregator лежит,
// события копятся в потоке и дождутся его — это и есть смысл очереди.
package publisher

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/WOLFraMix/sentinel/internal/events"
)

const StreamName = "incidents"
const SubjectAll = "incidents.>"

type Publisher struct {
	js jetstream.JetStream
}

// Connect создаёт поток, если его нет, и возвращает publisher.
// Retention = limits: события хранятся по времени/размеру, а не пока
// не прочитаны — история нужна и дашборду, а не только потребителю.
func Connect(ctx context.Context, url string) (*Publisher, error) {
	nc, err := nats.Connect(url, nats.Timeout(10*time.Second))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{SubjectAll},
		MaxAge:   7 * 24 * time.Hour, // неделя истории — стартовая политика
	})
	if err != nil {
		return nil, err
	}
	return &Publisher{js: js}, nil
}

// Publish сериализует и публикует событие в subject по его типу —
// темы вида incidents.pod_crash позволяют aggregator'у подписываться
// выборочно (только на крашы, игнорируя рестарты).
func (p *Publisher) Publish(ctx context.Context, e events.Event) error {
	e.PublishedAt = time.Now().UTC()
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	js, err := p.js.GetStream(ctx, StreamName)
	_ = js // асинхронная публикация через общий интерфейс:
	return p.publish(ctx, events.SubjectPrefix+string(e.Kind), data)
}

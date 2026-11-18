// Package storage — персистентность Sentinel: MongoDB.
//
// Две коллекции:
//
//	events    — сырой журнал всех событий (дедуп по уникальному id,
//	            TTL 30 дней: журнал с самозатуханием);
//	incidents — «живые» инциденты по одному на (кластер, ns, под,
//	            контейнер, вид): счётчик повторов и последнее состояние.
//
// Дедупликация двумя слоями: JetStream может доставить сообщение
// повторно (at-least-once), уникальный индекс по event.ID гарантирует,
// что дубль не попадёт в базу.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/WOLFraMix/sentinel/internal/events"
)

const (
	eventsCol    = "events"
	incidentsCol = "incidents"
)

type Storage struct {
	client    *mongo.Client
	db        *mongo.Database
	events    *mongo.Collection
	incidents *mongo.Collection
}

func Connect(ctx context.Context, url, dbName string) (*Storage, error) {
	// Драйвер сам управляет пулом соединений; таймауты берём из контекста.
	client, err := mongo.Connect(options.Client().ApplyURI(url))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	db := client.Database(dbName)
	s := &Storage{
		client:    client,
		db:        db,
		events:    db.Collection(eventsCol),
		incidents: db.Collection(incidentsCol),
	}
	if err := s.ensureIndexes(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// ensureIndexes — индексы создаются один раз, идемпотентно.
// Здесь живёт вся целостность: уникальность событий и инцидентов —
// на уровне базы, а не на надежде «пока никто не пишет параллельно».
func (s *Storage) ensureIndexes(ctx context.Context) error {
	// Уникальный ID события — главный дедуп.
	if _, err := s.events.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "id", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("events index: %w", err)
	}
	// TTL: MongoDB сама удаляет документы старше 30 дней по полю
	// occurred_at — журнал не требует обслуживания.
	if _, err := s.events.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "occurred_at", Value: 1}},
		Options: options.Index().
			SetExpireAfterSeconds(int32(30 * 24 * time.Hour / time.Second)),
	}); err != nil {
		return fmt.Errorf("events ttl: %w", err)
	}
	// Уникальный ключ инцидента.
	if _, err := s.incidents.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "key", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("incidents index: %w", err)
	}
	return nil
}

// SaveEvent вставляет событие в журнал. Возвращает true, если оно
// новое (не дубликат) — потребитель на этом решает, обновлять ли инцидент.
func (s *Storage) SaveEvent(ctx context.Context, e events.Event) (bool, error) {
	_, err := s.events.InsertOne(ctx, bson.M{
		"id":           e.ID,
		"kind":         string(e.Kind),
		"cluster":      e.Cluster,
		"namespace":    e.Namespace,
		"pod":          e.Pod,
		"container":    e.Container,
		"reason":       e.Reason,
		"message":      e.Message,
		"restarts":     e.Restarts,
		"occurred_at":  e.OccurredAt,
		"published_at": e.PublishedAt,
	})
	if mongo.IsDuplicateKeyError(err) {
		return false, nil // уже видели это событие — дубликат JetStream
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// BumpIncident — обновить или создать «живой» инцидент.
// Атомарный апсерт: счётчик растёт, last_* перезаписывается,
// first_seen ставится только при первом создании ($setOnInsert).
func (s *Storage) BumpIncident(ctx context.Context, e events.Event) error {
	key := fmt.Sprintf("%s/%s/%s/%s/%s",
		e.Cluster, e.Namespace, e.Pod, e.Container, e.Kind)
	_, err := s.incidents.UpdateOne(ctx,
		bson.M{"key": key},
		bson.M{
			"$inc": bson.M{"count": 1},
			"$set": bson.M{
				"kind":       string(e.Kind),
				"cluster":    e.Cluster,
				"namespace":  e.Namespace,
				"pod":        e.Pod,
				"container":  e.Container,
				"reason":     e.Reason,
				"message":    e.Message,
				"restarts":   e.Restarts,
				"last_seen":  e.OccurredAt,
				"updated_at": time.Now().UTC(),
			},
			"$setOnInsert": bson.M{"first_seen": e.OccurredAt},
		},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// Incident — проекция инцидента для дашборда и логов.
type Incident struct {
	Key       string    `bson:"key" json:"key"`
	Kind      string    `bson:"kind" json:"kind"`
	Cluster   string    `bson:"cluster" json:"cluster"`
	Namespace string    `bson:"namespace" json:"namespace"`
	Pod       string    `bson:"pod" json:"pod"`
	Container string    `bson:"container" json:"container"`
	Reason    string    `bson:"reason" json:"reason"`
	Message   string    `bson:"message" json:"message"`
	Restarts  int32     `bson:"restarts" json:"restarts"`
	Count     int64     `bson:"count" json:"count"`
	FirstSeen time.Time `bson:"first_seen" json:"first_seen"`
	LastSeen  time.Time `bson:"last_seen" json:"last_seen"`
	JiraKey   string    `bson:"jira_key" json:"jira_key,omitempty"`
}

// ListIncidents — последние инциденты по времени обновления.
// Сортировка в базе, а не в памяти: MongoDB держит индекс, а нам
// не нужно тащить всю коллекцию в Go.
func (s *Storage) ListIncidents(ctx context.Context, limit int) ([]Incident, error) {
	cur, err := s.incidents.Find(ctx, bson.M{},
		options.Find().SetSort(bson.D{{Key: "updated_at", Value: -1}}).
			SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []Incident
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Storage) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// GetIncident — инцидент по ключу; nil, если aggregator ещё не записал.
func (s *Storage) GetIncident(ctx context.Context, key string) (*Incident, error) {
	var inc Incident
	err := s.incidents.FindOne(ctx, bson.M{"key": key}).Decode(&inc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &inc, nil
}

// SetJiraKey — сохранить ключ тикета в инциденте: это дедуп тикетов.
func (s *Storage) SetJiraKey(ctx context.Context, key, ticketKey string) error {
	_, err := s.incidents.UpdateOne(ctx, bson.M{"key": key},
		bson.M{"$set": bson.M{"jira_key": ticketKey}})
	return err
}

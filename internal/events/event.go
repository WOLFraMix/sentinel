// Package events — контракт событий Sentinel.
// Единая структура с JSON-тегами — единственный способ договориться
// между watcher'ом, aggregator'ом и дашбордом без общего кода: каждый
// компонент живёт в своём пакете и знает только этот контракт.
package events

import "time"

// Kind — тип события. Список расширяемый: новые наблюдения
// добавляются константами, потребители не ломаются.
type Kind string

const (
	PodCrash     Kind = "pod_crash"   // контейнер упал (нестандартный выход)
	PodOOMKilled Kind = "pod_oom"     // убит OOM-killer'ом по памяти
	PodDeleted   Kind = "pod_deleted" // под удалён (владелец — заготовка итерации 2)
)

// SubjectPrefix — корень тем JetStream. Полная тема: incidents.<kind>,
// например incidents.pod_oom — aggregator может подписаться выборочно.
const SubjectPrefix = "incidents."

// Event — атомарное наблюдение из кластера.
// Все времена в UTC: у события нет «своей» таймзоны.
type Event struct {
	ID          string    `json:"id"` // дедуп-ключ: кластер+ns+под+контейнер+вид+счётчик
	Kind        Kind      `json:"kind"`
	Cluster     string    `json:"cluster"`
	Namespace   string    `json:"namespace"`
	Pod         string    `json:"pod"`
	Container   string    `json:"container,omitempty"`
	Reason      string    `json:"reason,omitempty"` // OOMKilled, Error, ExitCode...
	Message     string    `json:"message,omitempty"`
	Restarts    int32     `json:"restarts"`     // кумулятивный счётчик рестартов
	OccurredAt  time.Time `json:"occurred_at"`  // момент в кластере
	PublishedAt time.Time `json:"published_at"` // момент публикации
}

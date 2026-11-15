// Package events — контракт событий Sentinel.
// Единая структура и JSON-теги — единственный способ договориться
// между watcher'ом, aggregator'ом и дашбордом без общего кода.
package events

import "time"

// Kind — тип события кластера. Расширяемый список: новые проверки
// добавляются новыми константами, без ломки потребителей.
type Kind string

const (
	PodCrash    Kind = "pod_crash"    // под упал (CrashLoopBackOff, Error)
	PodOOMKilled Kind = "pod_oom"     // убит по памяти
	PodRestart  Kind = "pod_restart"  // аномально частые рестарты
	PodDeleted  Kind = "pod_deleted"  // удалён не по эволюции (заготовка)
)

// Event — атомарное наблюдение из кластера.
// Время — в UTC: у события нет «своей» таймзоны, у aggregator'а — своя.
type Event struct {
	ID          string    `json:"id"`           // уникальность: кластер+ns+под+вид
	Kind        Kind      `json:"kind"`
	Cluster     string    `json:"cluster"`
	Namespace   string    `json:"namespace"`
	Pod         string    `json:"pod"`
	Container   string    `json:"container,omitempty"`
	Reason      string    `json:"reason,omitempty"` // ExitCode, OOMKilled, ...
	Message     string    `json:"message,omitempty"`
	OccurredAt  time.Time `json:"occurred_at"`  // момент в кластере
	PublishedAt time.Time `json:"published_at"` // момент публикации
}

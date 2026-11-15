// Package watcher — наблюдение за подами кластера и публикация событий.
//
// Технология: SharedInformer из client-go. Это НЕ поллинг каждые N
// секунд: informer держит watch-соединение с API-сервером и получает
// изменения push'ем, а локальный кэш экономит запросы. При resync
// (периодической сверке) обработчики вызываются с old == new — наша
// логика сравнения счётчиков это отфильтрует: повторов не будет.
package watcher

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/WOLFraMix/sentinel/internal/events"
	"github.com/WOLFraMix/sentinel/internal/publisher"
)

type Watcher struct {
	pub     *publisher.Publisher
	cluster string
	ignore  map[string]struct{}
	log     *slog.Logger
}

func New(pub *publisher.Publisher, cluster string, ignored []string, log *slog.Logger) *Watcher {
	ign := make(map[string]struct{}, len(ignored))
	for _, ns := range ignored {
		ign[ns] = struct{}{}
	}
	return &Watcher{pub: pub, cluster: cluster, ignore: ign, log: log}
}

// ignored — проверка namespace из списка шумовых (kube-system и пр.):
// их поды падают по служебным причинам и только мусорят в инцидентах.
func (w *Watcher) ignored(ns string) bool {
	_, ok := w.ignore[ns]
	return ok
}

// Run запускает informer и блокируется до отмены контекста.
func (w *Watcher) Run(ctx context.Context, clientset kubernetes.Interface) error {
	// Resync каждые 30 секунд: страховка от пропущенных событий при
	// разрыве watch-соединения — полный проход по кэшу переведёт
	// «пропущенный рестарт» в «найденный на следующей сверке».
	factory := informers.NewSharedInformerFactory(clientset, 30*time.Second)
	informer := factory.Core().V1().Pods().Informer()

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		// Новый под в кэше: если он уже с рестартами — фиксируем сразу
		// (watcher стартовал, когда под уже падал).
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				w.onPod(ctx, nil, pod)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			old, ok1 := oldObj.(*corev1.Pod)
			new, ok2 := newObj.(*corev1.Pod)
			if ok1 && ok2 {
				w.onPod(ctx, old, new)
			}
		},
		// Удаление: событие «под исчез» — заготовка для корреляции
		// (aggregator свяжет удаление с предыдущими падениями).
		// Внимание: при resync-проходе удалений НЕ бывает — informer
		// не выкидывает существующие объекты, так что повторов нет.
		DeleteFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return // DeletedFinalStateUnknown из кэша — пропускаем
			}
			if w.ignored(pod.Namespace) {
				return
			}
			// Сигнатура emit требует container и reason — при удалении
			// пода они неизвестны, передаём пустые.
			w.emit(ctx, events.PodDeleted, pod.Namespace, pod.Name, "", "", "", 0, time.Now())
		},
	})
	if err != nil {
		return fmt.Errorf("add handlers: %w", err)
	}

	factory.Start(ctx.Done())
	// Ожидание остановки: у factory нет WaitForStop — блокируемся на
	// контексте. Когда ctx отменят (Ctrl+C в main), Start-горутины
	// завершатся сами, а мы выйдем из select и вернём ошибку контекста.
	<-ctx.Done()
	return ctx.Err()
}

// onPod — дифф двух состояний пода. Ключевая идея: события мы выводим
// не из «текущего состояния», а из ИЗМЕНЕНИЙ между состояниями —
// поэтому resync с old == new не порождает повторов.
func (w *Watcher) onPod(ctx context.Context, old, new *corev1.Pod) {
	if w.ignored(new.Namespace) {
		return
	}
	for i := range new.Status.ContainerStatuses {
		cs := &new.Status.ContainerStatuses[i]

		// Предыдущий счётчик рестартов этого контейнера.
		var prev int32
		if old != nil && i < len(old.Status.ContainerStatuses) {
			prev = old.Status.ContainerStatuses[i].RestartCount
		}
		if cs.RestartCount <= prev {
			continue // рестартов не прибавилось — ничего не делаем
		}

		// Причина последнего завершения контейнера — здесь лежат и
		// OOMKilled, и Error, и код выхода.
		reason, msg, finished := "", "", time.Now()
		if ls := cs.LastTerminationState.Terminated; ls != nil {
			reason = ls.Reason
			msg = ls.Message
			if !ls.FinishedAt.IsZero() {
				finished = ls.FinishedAt.Time
			}
		}
		kind := events.PodCrash
		if reason == "OOMKilled" {
			kind = events.PodOOMKilled
		}
		w.emit(ctx, kind, new.Namespace, new.Name, cs.Name, reason, msg, cs.RestartCount, finished)
	}
}

// emit — собрать событие и опубликовать. ID включает счётчик рестартов:
// один и тот же рестарт при повторном проходе даст тот же ID, а разные
// рестарты — разные. Агрегатор сможет дедуплицировать по ID.
func (w *Watcher) emit(ctx context.Context, kind events.Kind, ns, pod, container, reason, msg string, restarts int32, at time.Time) {
	id := fmt.Sprintf("%s/%s/%s/%s/%s/%d", w.cluster, ns, pod, container, kind, restarts)
	e := events.Event{
		ID:         id,
		Kind:       kind,
		Cluster:    w.cluster,
		Namespace:  ns,
		Pod:        pod,
		Container:  container,
		Reason:     reason,
		Message:    msg,
		Restarts:   restarts,
		OccurredAt: at.UTC(),
	}
	if err := w.pub.Publish(ctx, e); err != nil {
		w.log.Error("publish failed", "id", id, "err", err)
		return
	}
	w.log.Info("event published",
		"kind", e.Kind, "pod", ns+"/"+pod, "container", container,
		"reason", reason, "restarts", restarts)
}

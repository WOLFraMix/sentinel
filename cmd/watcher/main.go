// watcher — Sentinel-наблюдатель: следит за подами Kubernetes-кластера
// и публикует инциденты в NATS JetStream.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/WOLFraMix/sentinel/internal/config"
	"github.com/WOLFraMix/sentinel/internal/publisher"
	"github.com/WOLFraMix/sentinel/internal/watcher"
)

func main() {
	// Структурированные логи (slog) — level INFO, формат текстовый:
	// для демо читаемость важнее машинного парсинга.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	log := slog.Default()

	// .env для локальной разработки (в проде — только ENV).
	if _, err := os.Stat(".env"); err == nil {
		if data, err := os.ReadFile(".env"); err == nil {
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
	}
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pub, err := publisher.Connect(ctx, cfg.NATSURL)
	if err != nil {
		log.Error("nats", "err", err)
		os.Exit(1)
	}
	defer closeNATS(log)
	log.Info("nats connected", "url", cfg.NATSURL)

	clientset, err := kubeClient(cfg.Kubeconfig)
	if err != nil {
		log.Error("kubernetes client", "err", err)
		os.Exit(1)
	}

	// Job — не горутина: Run блокируется до отмены контекста.
	job := watcher.New(pub, cfg.ClusterName, cfg.IgnoredNamespaces, log)
	if err := job.Run(ctx, clientset); err != nil && ctx.Err() == nil {
		log.Error("watcher stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("watcher stopped gracefully")
}

// kubeClient — конфиг из kubeconfig (kind, локальные кластеры) с
// откатом на in-cluster (когда watcher сам запущен в поде).
func kubeClient(explicit string) (*kubernetes.Clientset, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if explicit != "" {
		rules.ExplicitPath = explicit
	}
	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err == nil {
		return kubernetes.NewForConfig(restCfg)
	}
	// Fallback: внутри пода kubeconfig нет — используем service account.
	inCluster, err2 := rest.InClusterConfig()
	if err2 != nil {
		return nil, err // возвращаем исходную ошибку — она информативнее
	}
	return kubernetes.NewForConfig(inCluster)
}

func closeNATS(log *slog.Logger) {
	// Закрытие соединения через publisher требует expose — оставим
	// процессу завершиться естественным образом: соединение умрёт с
	// процессом. Для наблюдателя это допустимо (следующая итерация
	// введёт close() в интерфейс, когда появится второй компонент).
}

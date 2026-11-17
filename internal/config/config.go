// Package config — конфигурация компонента из окружения.
// Правило то же, что в Recall: дефолты в коде согласованы с
// .env.example, файл .env — для локальной разработки, в проде —
// только переменные окружения.
package config

import (
	"os"
	"strings"
)

type Config struct {
	NATSURL           string   // адрес брокера событий
	ClusterName       string   // имя наблюдаемого кластера
	IgnoredNamespaces []string // шумовые namespace
	Kubeconfig        string   // явный путь к kubeconfig (для тестов)
	MongoURL          string   // mongodb://user:pass@host:port
	MongoDB           string   // имя базы
	APIAddr           string   // адрес дашборда, например :8080
}

func Load() Config {
	return Config{
		NATSURL:           getEnv("SNT_NATS_URL", "nats://127.0.0.1:4222"),
		ClusterName:       getEnv("SNT_CLUSTER_NAME", "default"),
		IgnoredNamespaces: splitCSV(getEnv("SNT_IGNORE_NS", "kube-system")),
		Kubeconfig:        getEnv("SNT_KUBECONFIG", ""),
		MongoURL:          getEnv("SNT_MONGO_URL", "mongodb://sentinel:sentinel@127.0.0.1:27017"),
		MongoDB:           getEnv("SNT_MONGO_DB", "sentinel"),
		APIAddr:           getEnv("SNT_API_ADDR", ":8080"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Package config — конфигурация компонента из окружения.
// Правило то же, что в Recall: дефолты в коде согласованы с
// .env.example, файл .env — для локальной разработки, в проде —
// только переменные окружения.
package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	NATSURL           string   // адрес брокера событий
	ClusterName       string   // имя наблюдаемого кластера
	IgnoredNamespaces []string // шумовые namespace
	Kubeconfig        string   // явный путь к kubeconfig (для тестов)
}

func Load() Config {
	return Config{
		NATSURL:           getEnv("SNT_NATS_URL", "nats://127.0.0.1:4222"),
		ClusterName:       getEnv("SNT_CLUSTER_NAME", "default"),
		IgnoredNamespaces: splitCSV(getEnv("SNT_IGNORE_NS", "kube-system")),
		Kubeconfig:        getEnv("SNT_KUBECONFIG", ""),
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

// strconv импортирован заранее: понадобятся числовые параметры
// (интервалы, пороги) на следующих итерациях — не буду плодить правки.
var _ = strconv.Itoa

// Package env — загрузка .env-файла в переменные окружения.
// Существующие переменные окружения имеют приоритет: файл
// заполняет только пустые — так можно переопределять значения
// для тестов и CI, не трогая .env.
package env

import (
	"os"
	"strings"
)

func Load(path string) {
	if _, err := os.Stat(path); err != nil {
		return
	}
	data, _ := os.ReadFile(path)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			k = strings.TrimSpace(k)
			v = strings.Trim(strings.TrimSpace(v), `"'`)
			if os.Getenv(k) == "" {
				_ = os.Setenv(k, v)
			}
		}
	}
}

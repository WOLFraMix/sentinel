// Package api — веб-дашборд Sentinel.
//
// Два канала данных на фронтенд:
//
//	GET /api/incidents — разовый снимок (используется при загрузке страницы);
//	GET /api/stream    — SSE-поток: сервер раз в 2 секунды делает свежий
//	                     снимок и пушит его всем подписчикам. Это
//	                     осознанный упрощённый вариант: честный push
//	                     потребовал бы связки aggregator → api через NATS —
//	                     сделаем позже, если захочется.
package api

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/WOLFraMix/sentinel/internal/storage"
)

//go:embed web
var webFS embed.FS

type Server struct {
	st  *storage.Storage
	log *slog.Logger
}

func New(st *storage.Storage, log *slog.Logger) *Server {
	return &Server{st: st, log: log}
}

// Run поднимает HTTP-сервер и блокируется до отмены контекста.
func (s *Server) Run(ctx context.Context, addr string) error {
	gin.SetMode(gin.ReleaseMode) // без дебаг-шума в логах
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "time": time.Now().UTC()})
	})
	r.GET("/api/incidents", s.listIncidents)
	r.GET("/api/stream", s.stream)

	// Статика дашборда: web/ распаковывается из бинарника —
	// запускаемый файл один, никаких «не забудь скопировать html».
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		return err
	}
	r.NoRoute(func(c *gin.Context) {
		// Всё, что не API, отдаём из встроенной статики.
		c.FileFromFS(c.Request.URL.Path, http.FS(sub))
	})

	srv := &http.Server{Addr: addr, Handler: r}

	// Graceful shutdown: Ctrl+C → ctx отменяется → серверу даём 5 секунд
	// дочитать текущие запросы, потом выходим.
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// listIncidents — свежий список инцидентов для первой отрисовки.
func (s *Server) listIncidents(c *gin.Context) {
	list, err := s.st.ListIncidents(c.Request.Context(), 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, list)
}

// stream — SSE: подписка на обновления списка.
func (s *Server) stream(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return // клиент ушёл — поток закрыт
		case <-ticker.C:
			list, err := s.st.ListIncidents(c.Request.Context(), 100)
			if err != nil {
				continue // осечка базы — пропустим тик, не рвём поток
			}
			data, err := json.Marshal(list)
			if err != nil {
				continue
			}
			if _, err := c.Writer.WriteString("data: " + string(data) + "\n\n"); err != nil {
				return
			}
			c.Writer.Flush()
		}
	}
}

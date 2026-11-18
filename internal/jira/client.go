// Package jira — REST-клиент Jira Cloud API v3.
//
// Особенности v3: description и комментарии передаются в ADF
// (Atlassian Document Format) — вложенный JSON; строка вместо ADF
// даёт 400. Авторизация — Basic: email + API-токен.
//
// Идемпотентность: каждый тикет несёт уникальную метку, выведенную
// из ключа инцидента. Сети до Atlassian свойственно рвать соединения
// после того, как запрос уже обработан на сервере (тикет создан,
// а ответ не дошёл) — поэтому перед созданием мы ищем тикет по метке
// и не создаём дубль.
package jira

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	base    string // https://xxx.atlassian.net
	email   string
	token   string
	project string
	http    *http.Client
}

func New(base, email, token, project string) *Client {
	return &Client{
		base:    base,
		email:   email,
		token:   token,
		project: project,
		// Таймаут обязателен: без него зависший Jira повесит notifier.
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

// LabelFor — уникальная метка инцидента для идемпотентности:
// тикет с такой меткой может существовать только один.
// В labels Jira запрещены слэши и пробелы, поэтому ключ инцидента
// хэшируем и берём первые 8 байт.
func LabelFor(incidentKey string) string {
	sum := sha256.Sum256([]byte(incidentKey))
	return "sentinel-" + fmt.Sprintf("%x", sum[:8])
}

// adf — минимальный ADF-документ из одной строки текста.
func adf(text string) map[string]any {
	return map[string]any{
		"type":    "doc",
		"version": 1,
		"content": []any{
			map[string]any{
				"type": "paragraph",
				"content": []any{
					map[string]any{"type": "text", "text": text},
				},
			},
		},
	}
}

type createResp struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

type searchResp struct {
	Issues []struct {
		Key string `json:"key"`
	} `json:"issues"`
}

type errResp struct {
	Messages []string          `json:"errorMessages"`
	Errors   map[string]string `json:"errors"`
}

// FindByLabel — найти тикет по метке; пустая строка = не найден.
func (c *Client) FindByLabel(ctx context.Context, label string) (string, error) {
	jql := fmt.Sprintf("project = %s AND labels = %q ORDER BY created DESC", c.project, label)
	payload := map[string]any{
		"jql":        jql,
		"maxResults": 1,
		"fields":     []string{"key"},
	}
	var out searchResp
	if err := c.do(ctx, http.MethodPost, "/rest/api/3/search/jql", payload, &out); err != nil {
		return "", err
	}
	if len(out.Issues) == 0 {
		return "", nil
	}
	return out.Issues[0].Key, nil
}

// CreateIssue — создать тикет, вернуть его ключ (например SNT-1).
// label — метка идемпотентности; создающий код обязан перед этим
// вызвать FindByLabel и убедиться, что тикета ещё нет.
func (c *Client) CreateIssue(ctx context.Context, issueType, summary, description, label string) (string, error) {
	payload := map[string]any{
		"fields": map[string]any{
			"project":     map[string]any{"key": c.project},
			"issuetype":   map[string]any{"name": issueType},
			"summary":     summary,
			"description": adf(description),
			"labels":      []string{label},
		},
	}
	var out createResp
	if err := c.do(ctx, http.MethodPost, "/rest/api/3/issue", payload, &out); err != nil {
		return "", err
	}
	return out.Key, nil
}

// AddComment — добавить комментарий к существующему тикету.
func (c *Client) AddComment(ctx context.Context, issueKey, text string) error {
	payload := map[string]any{"body": adf(text)}
	return c.do(ctx, http.MethodPost,
		"/rest/api/3/issue/"+issueKey+"/comment", payload, nil)
}

func (c *Client) do(ctx context.Context, method, path string, payload any, out any) error {
	var buf bytes.Buffer
	if payload != nil {
		if err := json.NewEncoder(&buf).Encode(payload); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, &buf)
	if err != nil {
		return err
	}
	// Basic: email:token — пароли Jira Cloud не принимает с 2019 года.
	req.SetBasicAuth(c.email, c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out == nil || resp.StatusCode == http.StatusNoContent {
			io.Copy(io.Discard, resp.Body)
			return nil
		}
		return json.NewDecoder(resp.Body).Decode(out)
	}

	// Jira отдаёт ошибки человекочитаемо: errorMessages + errors.
	var e errResp
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if json.Unmarshal(body, &e) == nil && (len(e.Messages) > 0 || len(e.Errors) > 0) {
		return fmt.Errorf("jira %d: %v %v", resp.StatusCode, e.Messages, e.Errors)
	}
	return fmt.Errorf("jira %d: %s", resp.StatusCode, body)
}

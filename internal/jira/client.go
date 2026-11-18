// Package jira — REST-клиент Jira Cloud API v3.
//
// Особенности v3: description и комментарии передаются в ADF
// (Atlassian Document Format) — вложенный JSON; строка вместо ADF
// даёт 400. Авторизация — Basic: email + API-токен.
package jira

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	base  string // https://xxx.atlassian.net
	email string
	token string
	http  *http.Client
}

func New(base, email, token string) *Client {
	return &Client{
		base:  base,
		email: email,
		token: token,
		// Таймаут обязателен: без него зависший Jira повесит notifier.
		http: &http.Client{Timeout: 15 * time.Second},
	}
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

type errResp struct {
	Messages []string          `json:"errorMessages"`
	Errors   map[string]string `json:"errors"`
}

// CreateIssue — создать тикет, вернуть его ключ (например SNT-1).
func (c *Client) CreateIssue(ctx context.Context, project, issueType, summary, description string) (string, error) {
	payload := map[string]any{
		"fields": map[string]any{
			"project":     map[string]any{"key": project},
			"issuetype":   map[string]any{"name": issueType},
			"summary":     summary,
			"description": adf(description),
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

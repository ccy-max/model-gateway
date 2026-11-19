package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// upstream OpenAI-compatible 上游客户端。
// 现有 Qoder 服务(:2323)本身就是兼容端点，因此它也走本客户端。
// 密钥不固定在客户端上：每次调用按供应商密钥池轮询选出后传入。
type upstream struct {
	id      string
	name    string
	baseURL string
	http    *http.Client
}

// newUpstream 规范化 BaseURL（自动补 /v1）并构建客户端。
func newUpstream(p Provider, chatTimeout, modelsTimeout time.Duration) *upstream {
	base := strings.TrimRight(strings.TrimSpace(p.BaseURL), "/")
	if base != "" && !strings.HasSuffix(base, "/v1") {
		base += "/v1"
	}
	return &upstream{
		id:      p.ID,
		name:    p.Name,
		baseURL: base,
		http: &http.Client{
			Timeout: chatTimeout,
		},
	}
}

// Chat 调用上游 chat/completions。body 为客户端原始请求体（map 形式），
// 仅覆写 model 与 stream，其余参数（temperature/tools/reasoning_effort 等）原样透传。
// 流式时默认注入 stream_options.include_usage 以便统计 token。
// key 由调用方按密钥池轮询选出。
func (u *upstream) Chat(ctx context.Context, model string, body map[string]any, stream bool, key string) (*http.Response, error) {
	payload := make(map[string]any, len(body)+2)
	for k, v := range body {
		payload[k] = v
	}
	payload["model"] = model
	payload["stream"] = stream
	if stream {
		if _, ok := payload["stream_options"]; !ok {
			payload["stream_options"] = map[string]any{"include_usage": true}
		}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.baseURL+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return u.http.Do(req)
}

// remoteModel 上游目录条目。
type remoteModel struct {
	ID   string `json:"id"`
	Caps []string
}

// Models 拉取上游模型目录 GET /models（用指定 key）。
func (u *upstream) Models(ctx context.Context, key string) ([]remoteModel, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.baseURL+"/models", nil)
	if err != nil {
		return nil, err
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("上游返回 %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var parsed struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
		Models []struct {
			ID string `json:"id"`
		} `json:"models"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("目录解析失败: %w", err)
	}
	seen := map[string]bool{}
	out := make([]remoteModel, 0, len(parsed.Data)+len(parsed.Models))
	for _, m := range parsed.Data {
		if m.ID != "" && !seen[m.ID] {
			seen[m.ID] = true
			out = append(out, remoteModel{ID: m.ID, Caps: guessCaps(m.ID)})
		}
	}
	for _, m := range parsed.Models {
		if m.ID != "" && !seen[m.ID] {
			seen[m.ID] = true
			out = append(out, remoteModel{ID: m.ID, Caps: guessCaps(m.ID)})
		}
	}
	return out, nil
}

// guessCaps 按模型名启发式推断能力（仅供面板参考，最终以人工勾选为准）。
// 非对话模型（embed/tts/图像/审核等）标注 "other"，面板默认不勾选。
func guessCaps(name string) []string {
	n := strings.ToLower(name)
	caps := []string{"chat", "tools"}
	switch {
	case containsAny(n, "embed", "embedding", "whisper", "tts", "dall-e", "dalle",
		"moderation", "rerank", "ocr", "asr", "speech", "image-gen", "stable-diffusion", "sdxl"):
		return []string{"other"}
	}
	if containsAny(n, "o1", "o3", "o4", "reasoning", "thinking", "-r1", "qwq") {
		caps = append(caps, "reasoning")
	}
	if containsAny(n, "4o", "vision", "-vl", "omni", "multimodal", "gemini", "claude") {
		caps = append(caps, "vision")
	}
	return caps
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// upstreamError 从上游响应体提取可读错误信息。
func upstreamError(status int, body []byte) string {
	var parsed struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil {
		if parsed.Error.Message != "" {
			return parsed.Error.Message
		}
		if parsed.Message != "" {
			return parsed.Message
		}
	}
	return truncate(string(body), 300)
}

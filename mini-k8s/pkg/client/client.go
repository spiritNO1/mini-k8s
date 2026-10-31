// Package client  mini-k8s apiserver  Go
//
//	schedulerkubelet  minictl
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	v1 "github.com/sunpeng/mini-k8s/pkg/apis/core/v1"
)

// Client  HTTP  apiserver
type Client struct {
	baseURL string
	http    *http.Client
}

// New server  "http://127.0.0.1:8080"
func New(server string) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(server, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// ------------------------- Pod -------------------------

// CreatePod  Pod
func (c *Client) CreatePod(ctx context.Context, pod *v1.Pod) (*v1.Pod, error) {
	var out v1.Pod
	if err := c.do(ctx, http.MethodPost, "/api/v1/pods", pod, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListPods  Pod
func (c *Client) ListPods(ctx context.Context) ([]v1.Pod, error) {
	var out v1.PodList
	if err := c.do(ctx, http.MethodGet, "/api/v1/pods", nil, &out); err != nil {
		return nil, err
	}
	return out.Items, nil
}

// GetPod  Pod
func (c *Client) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	var out v1.Pod
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", ns(namespace), name)
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdatePod  Pod spec/status
func (c *Client) UpdatePod(ctx context.Context, pod *v1.Pod) (*v1.Pod, error) {
	var out v1.Pod
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", ns(pod.Namespace), pod.Name)
	if err := c.do(ctx, http.MethodPut, path, pod, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeletePod   Pod
func (c *Client) DeletePod(ctx context.Context, namespace, name string) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", ns(namespace), name)
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// ------------------------- Node -------------------------

// RegisterNode
func (c *Client) RegisterNode(ctx context.Context, node *v1.Node) error {
	return c.do(ctx, http.MethodPost, "/api/v1/nodes", node, nil)
}

// ListNodes
func (c *Client) ListNodes(ctx context.Context) ([]v1.Node, error) {
	var out v1.NodeList
	if err := c.do(ctx, http.MethodGet, "/api/v1/nodes", nil, &out); err != nil {
		return nil, err
	}
	return out.Items, nil
}

// ------------------------- Watch -------------------------

// PodEvent  Pod watch
type PodEvent struct {
	Type v1.WatchEventType
	Pod  *v1.Pod
}

// wireEvent   apiserver  ndjson
type wireEvent struct {
	Type   v1.WatchEventType `json:"type"`
	Object json.RawMessage   `json:"object"`
}

// WatchPods  Pod
//
//	channel  ctx errCh
func (c *Client) WatchPods(ctx context.Context) (<-chan PodEvent, <-chan error) {
	events := make(chan PodEvent)
	errCh := make(chan error, 1)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/watch/pods", nil)
	if err != nil {
		errCh <- err
		close(events)
		return events, errCh
	}

	// watch
	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		errCh <- err
		close(events)
		return events, errCh
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		errCh <- fmt.Errorf("watch pods: unexpected status %s", resp.Status)
		close(events)
		return events, errCh
	}

	go func() {
		defer resp.Body.Close()
		defer close(events)
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				trimmed := bytes.TrimSpace(line)
				if len(trimmed) > 0 {
					var we wireEvent
					if jerr := json.Unmarshal(trimmed, &we); jerr == nil && len(we.Object) > 0 {
						var pod v1.Pod
						if perr := json.Unmarshal(we.Object, &pod); perr == nil {
							select {
							case events <- PodEvent{Type: we.Type, Pod: &pod}:
							case <-ctx.Done():
								return
							}
						}
					}
				}
			}
			if err != nil {
				if err != io.EOF && ctx.Err() == nil {
					errCh <- err
				}
				return
			}
		}
	}()

	return events, errCh
}

// ------------------------- internals -------------------------

// do  HTTP body out
func (c *Client) do(ctx context.Context, method, path string, body, out interface{}) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s -> %s: %s", method, path, resp.Status, strings.TrimSpace(string(respBytes)))
	}
	if out != nil && len(respBytes) > 0 {
		return json.Unmarshal(respBytes, out)
	}
	return nil
}

func ns(namespace string) string {
	if namespace == "" {
		return "default"
	}
	return namespace
}

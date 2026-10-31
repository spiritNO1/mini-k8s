// Command mini-k8s  apiserverscheduler  kubelet
//
//	Pod
//
//	HTTP  apiserver  k8s
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sunpeng/mini-k8s/pkg/apiserver"
	"github.com/sunpeng/mini-k8s/pkg/client"
	"github.com/sunpeng/mini-k8s/pkg/kubelet"
	"github.com/sunpeng/mini-k8s/pkg/runtime"
	"github.com/sunpeng/mini-k8s/pkg/scheduler"
	"github.com/sunpeng/mini-k8s/pkg/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "apiserver ")
	nodeName := flag.String("node-name", defaultNodeName(), "")
	logDir := flag.String("logdir", envOr("MINI_K8S_LOG_DIR", "/tmp/mini-k8s/logs"), "")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1) apiserver store
	st := store.New()
	srv := apiserver.New(st)
	httpServer := &http.Server{Addr: *addr, Handler: srv.Handler()}
	go func() {
		log.Printf("[apiserver] listening on %s", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("apiserver exited: %v", err)
		}
	}()

	//  apiserver
	serverURL := "http://" + *addr
	c := client.New(serverURL)
	if err := waitForAPI(ctx, c); err != nil {
		log.Fatalf("apiserver not ready: %v", err)
	}

	// 2) scheduler
	go scheduler.New(c, 2*time.Second).Run(ctx)

	// 3) kubelet
	rt, err := runtime.New(*logDir)
	if err != nil {
		log.Fatalf("init runtime failed: %v", err)
	}
	go kubelet.New(*nodeName, c, rt, 2*time.Second, 5*time.Second).Run(ctx)

	log.Printf("mini-k8s all-in-one ready.  minictl --server %s ", serverURL)

	<-ctx.Done()
	log.Printf("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

// waitForAPI  /healthz  apiserver
func waitForAPI(ctx context.Context, c *client.Client) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.ListNodes(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return context.DeadlineExceeded
}

func defaultNodeName() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "mini-k8s-node"
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

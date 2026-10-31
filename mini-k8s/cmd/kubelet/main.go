// Command kubelet  mini-k8s  Pod
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sunpeng/mini-k8s/pkg/client"
	"github.com/sunpeng/mini-k8s/pkg/kubelet"
	"github.com/sunpeng/mini-k8s/pkg/runtime"
)

func main() {
	server := flag.String("server", envOr("MINI_K8S_SERVER", "http://127.0.0.1:8080"), "apiserver ")
	nodeName := flag.String("node-name", defaultNodeName(), "")
	logDir := flag.String("logdir", envOr("MINI_K8S_LOG_DIR", "/tmp/mini-k8s/logs"), "")
	syncInterval := flag.Duration("sync-interval", 2*time.Second, "Pod ")
	heartbeatInterval := flag.Duration("heartbeat-interval", 5*time.Second, "")
	flag.Parse()

	rt, err := runtime.New(*logDir)
	if err != nil {
		log.Fatalf("init runtime failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := client.New(*server)
	k := kubelet.New(*nodeName, c, rt, *syncInterval, *heartbeatInterval)

	log.Printf("kubelet connecting to apiserver %s (logdir=%s)", *server, *logDir)
	k.Run(ctx)
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

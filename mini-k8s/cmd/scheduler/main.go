// Command scheduler  mini-k8s  Pod  Ready
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
	"github.com/sunpeng/mini-k8s/pkg/scheduler"
)

func main() {
	server := flag.String("server", envOr("MINI_K8S_SERVER", "http://127.0.0.1:8080"), "apiserver ")
	interval := flag.Duration("interval", 2*time.Second, "")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := client.New(*server)
	s := scheduler.New(c, *interval)

	log.Printf("scheduler connecting to apiserver %s", *server)
	s.Run(ctx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

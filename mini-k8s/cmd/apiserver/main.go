// Command apiserver  mini-k8s  API  store  REST/Watch
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
	"github.com/sunpeng/mini-k8s/pkg/store"
)

func main() {
	addr := flag.String("addr", ":8080", "")
	flag.Parse()

	st := store.New()
	srv := apiserver.New(st)

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: srv.Handler(),
	}

	//
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("mini-k8s apiserver listening on %s", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("apiserver exited: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down apiserver...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

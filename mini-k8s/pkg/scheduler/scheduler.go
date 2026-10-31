// Package scheduler  mini-k8s
//
//	Pod Ready  spec.nodeName
//	k8s  Filter/Score
//
// round-robin
package scheduler

import (
	"context"
	"log"
	"time"

	v1 "github.com/sunpeng/mini-k8s/pkg/apis/core/v1"
	"github.com/sunpeng/mini-k8s/pkg/client"
)

// Scheduler  Pending  Pod
type Scheduler struct {
	client   *client.Client
	interval time.Duration
	next     int // round-robin
}

// New
func New(c *client.Client, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Scheduler{client: c, interval: interval}
}

// Run  ctx
func (s *Scheduler) Run(ctx context.Context) {
	log.Printf("scheduler started (interval=%s)", s.interval)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	//
	s.scheduleOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Printf("scheduler stopped")
			return
		case <-ticker.C:
			s.scheduleOnce(ctx)
		}
	}
}

func (s *Scheduler) scheduleOnce(ctx context.Context) {
	pods, err := s.client.ListPods(ctx)
	if err != nil {
		log.Printf("scheduler: list pods failed: %v", err)
		return
	}

	//  Pod
	var pending []v1.Pod
	for _, p := range pods {
		if p.Spec.NodeName == "" && p.Status.Phase != v1.PodSucceeded && p.Status.Phase != v1.PodFailed {
			pending = append(pending, p)
		}
	}
	if len(pending) == 0 {
		return
	}

	nodes, err := s.client.ListNodes(ctx)
	if err != nil {
		log.Printf("scheduler: list nodes failed: %v", err)
		return
	}
	var ready []v1.Node
	for _, n := range nodes {
		if n.Status.Phase == v1.NodeReady {
			ready = append(ready, n)
		}
	}
	if len(ready) == 0 {
		log.Printf("scheduler: %d pod(s) pending but no ready node available", len(pending))
		return
	}

	for _, p := range pending {
		node := ready[s.next%len(ready)]
		s.next++

		p.Spec.NodeName = node.Name
		if _, err := s.client.UpdatePod(ctx, &p); err != nil {
			log.Printf("scheduler: bind pod %s/%s to node %s failed: %v", p.Namespace, p.Name, node.Name, err)
			continue
		}
		log.Printf("scheduler: bound pod %s/%s -> node %s", p.Namespace, p.Name, node.Name)
	}
}

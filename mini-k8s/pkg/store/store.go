// Package store  mini-k8s  apiserver  "etcd"

//  resourceVersion  CRUD  channel  Watch

package store

import (
	"fmt"

	"sort"

	"sync"

	"time"

	v1 "github.com/sunpeng/mini-k8s/pkg/apis/core/v1"
)

// Resource   watch

type Resource string

const (
	ResourcePods Resource = "pods"

	ResourceNodes Resource = "nodes"
)

// watcher

type watcher struct {
	id int64

	resource Resource

	ch chan v1.WatchEvent
}

// Store

type Store struct {
	mu sync.RWMutex

	pods map[string]*v1.Pod

	nodes map[string]*v1.Node

	resourceVersion int64

	watchers map[int64]*watcher

	nextWatchID int64
}

// New  Store

func New() *Store {

	return &Store{

		pods: make(map[string]*v1.Pod),

		nodes: make(map[string]*v1.Node),

		watchers: make(map[int64]*watcher),

		resourceVersion: 0,
	}

}

// Key  store  namespace/name

func Key(namespace, name string) string {

	if namespace == "" {

		namespace = "default"

	}

	return namespace + "/" + name

}

// bumpVersion  resourceVersion

func (s *Store) bumpVersion() int64 {

	s.resourceVersion++

	return s.resourceVersion

}

// broadcast  watcher

func (s *Store) broadcast(resource Resource, event v1.WatchEvent) {

	for _, w := range s.watchers {

		if w.resource != resource {

			continue

		}

		select {

		case w.ch <- event:

		default:

			// watcher  store

			//  k8s  410 Gone  list

		}

	}

}

// ------------------------- Pod  -------------------------

// CreatePod  Pod

func (s *Store) CreatePod(pod *v1.Pod) (*v1.Pod, error) {

	s.mu.Lock()

	defer s.mu.Unlock()

	if pod.Namespace == "" {

		pod.Namespace = "default"

	}

	k := Key(pod.Namespace, pod.Name)

	if _, ok := s.pods[k]; ok {

		return nil, fmt.Errorf("pod %q already exists", k)

	}

	pod.CreationTimestamp = time.Now()

	pod.ResourceVersion = s.bumpVersion()

	if pod.Status.Phase == "" {

		pod.Status.Phase = v1.PodPending

	}

	stored := pod.DeepCopy()

	s.pods[k] = stored

	s.broadcast(ResourcePods, v1.WatchEvent{Type: v1.WatchAdded, Object: stored.DeepCopy()})

	return stored.DeepCopy(), nil

}

// UpdatePod  Pod

func (s *Store) UpdatePod(pod *v1.Pod) (*v1.Pod, error) {

	s.mu.Lock()

	defer s.mu.Unlock()

	k := Key(pod.Namespace, pod.Name)

	if _, ok := s.pods[k]; !ok {

		return nil, fmt.Errorf("pod %q not found", k)

	}

	pod.ResourceVersion = s.bumpVersion()

	stored := pod.DeepCopy()

	s.pods[k] = stored

	s.broadcast(ResourcePods, v1.WatchEvent{Type: v1.WatchModified, Object: stored.DeepCopy()})

	return stored.DeepCopy(), nil

}

// GetPod  namespace/name  Pod

func (s *Store) GetPod(namespace, name string) (*v1.Pod, error) {

	s.mu.RLock()

	defer s.mu.RUnlock()

	k := Key(namespace, name)

	pod, ok := s.pods[k]

	if !ok {

		return nil, fmt.Errorf("pod %q not found", k)

	}

	return pod.DeepCopy(), nil

}

// DeletePod   Pod

func (s *Store) DeletePod(namespace, name string) (*v1.Pod, error) {

	s.mu.Lock()

	defer s.mu.Unlock()

	k := Key(namespace, name)

	pod, ok := s.pods[k]

	if !ok {

		return nil, fmt.Errorf("pod %q not found", k)

	}

	delete(s.pods, k)

	s.broadcast(ResourcePods, v1.WatchEvent{Type: v1.WatchDeleted, Object: pod.DeepCopy()})

	return pod.DeepCopy(), nil

}

// ListPods  Pod

func (s *Store) ListPods() []*v1.Pod {

	s.mu.RLock()

	defer s.mu.RUnlock()

	list := make([]*v1.Pod, 0, len(s.pods))

	for _, p := range s.pods {

		list = append(list, p.DeepCopy())

	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list

}

// ------------------------- Node  -------------------------

// UpsertNode

func (s *Store) UpsertNode(node *v1.Node) (*v1.Node, error) {

	s.mu.Lock()

	defer s.mu.Unlock()

	k := Key("", node.Name)

	node.Status.LastHeartbeat = time.Now()

	node.ResourceVersion = s.bumpVersion()

	eventType := v1.WatchModified

	if _, ok := s.nodes[k]; !ok {

		eventType = v1.WatchAdded

	}

	stored := *node

	s.nodes[k] = &stored

	s.broadcast(ResourceNodes, v1.WatchEvent{Type: eventType, Object: copyNode(&stored)})

	return copyNode(&stored), nil

}

// GetNode

func (s *Store) GetNode(name string) (*v1.Node, error) {

	s.mu.RLock()

	defer s.mu.RUnlock()

	node, ok := s.nodes[Key("", name)]

	if !ok {

		return nil, fmt.Errorf("node %q not found", name)

	}

	return copyNode(node), nil

}

// DeleteNode

func (s *Store) DeleteNode(name string) {

	s.mu.Lock()

	defer s.mu.Unlock()

	k := Key("", name)

	if node, ok := s.nodes[k]; ok {

		delete(s.nodes, k)

		s.broadcast(ResourceNodes, v1.WatchEvent{Type: v1.WatchDeleted, Object: copyNode(node)})

	}

}

// ListNodes

func (s *Store) ListNodes() []*v1.Node {

	s.mu.RLock()

	defer s.mu.RUnlock()

	list := make([]*v1.Node, 0, len(s.nodes))

	for _, n := range s.nodes {

		list = append(list, copyNode(n))

	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list

}

func copyNode(n *v1.Node) *v1.Node {

	out := *n

	return &out

}

// ------------------------- Watch -------------------------

// Watcher  watch

type Watcher struct {
	store *Store

	w *watcher
}

// ResultChan

func (wt *Watcher) ResultChan() <-chan v1.WatchEvent {

	return wt.w.ch

}

// Stop  watcher

func (wt *Watcher) Stop() {

	wt.store.stopWatch(wt.w.id)

}

// Watch  watcher ADDED

func (s *Store) Watch(resource Resource, sendInitial bool) *Watcher {

	s.mu.Lock()

	s.nextWatchID++

	w := &watcher{

		id: s.nextWatchID,

		resource: resource,

		ch: make(chan v1.WatchEvent, 128),
	}

	s.watchers[w.id] = w

	var initial []v1.WatchEvent

	if sendInitial {

		switch resource {

		case ResourcePods:

			for _, p := range s.pods {

				initial = append(initial, v1.WatchEvent{Type: v1.WatchAdded, Object: p.DeepCopy()})

			}

		case ResourceNodes:

			for _, n := range s.nodes {

				initial = append(initial, v1.WatchEvent{Type: v1.WatchAdded, Object: copyNode(n)})

			}

		}

	}

	s.mu.Unlock()

	//

	for _, e := range initial {

		select {

		case w.ch <- e:

		default:

		}

	}

	return &Watcher{store: s, w: w}

}

func (s *Store) stopWatch(id int64) {

	s.mu.Lock()

	defer s.mu.Unlock()

	if w, ok := s.watchers[id]; ok {

		close(w.ch)

		delete(s.watchers, id)

	}

}

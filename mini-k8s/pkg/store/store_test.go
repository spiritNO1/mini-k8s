package store

import (
	"testing"
	"time"

	v1 "github.com/sunpeng/mini-k8s/pkg/apis/core/v1"
)

func TestPodCRUD(t *testing.T) {
	s := New()

	pod := &v1.Pod{ObjectMeta: v1.ObjectMeta{Name: "p1", Namespace: "default"}}
	created, err := s.CreatePod(pod)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.UID == "" && created.ResourceVersion == 0 {
		t.Fatalf("expected resourceVersion to be set")
	}
	if created.Status.Phase != v1.PodPending {
		t.Fatalf("expected initial phase Pending, got %s", created.Status.Phase)
	}

	// 重复创建应失败。
	if _, err := s.CreatePod(&v1.Pod{ObjectMeta: v1.ObjectMeta{Name: "p1"}}); err == nil {
		t.Fatalf("expected error on duplicate create")
	}

	got, err := s.GetPod("default", "p1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got.Status.Phase = v1.PodRunning
	if _, err := s.UpdatePod(got); err != nil {
		t.Fatalf("update: %v", err)
	}

	again, _ := s.GetPod("default", "p1")
	if again.Status.Phase != v1.PodRunning {
		t.Fatalf("expected phase Running after update, got %s", again.Status.Phase)
	}

	// resourceVersion 应随更新递增。
	if again.ResourceVersion <= created.ResourceVersion {
		t.Fatalf("resourceVersion should increase: %d -> %d", created.ResourceVersion, again.ResourceVersion)
	}

	if _, err := s.DeletePod("default", "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetPod("default", "p1"); err == nil {
		t.Fatalf("expected not found after delete")
	}
}

// TestReturnedObjectsAreCopies 确认 store 返回的是副本，外部修改不会污染内部状态。
func TestReturnedObjectsAreCopies(t *testing.T) {
	s := New()
	_, _ = s.CreatePod(&v1.Pod{ObjectMeta: v1.ObjectMeta{Name: "p", Namespace: "default"}})

	got, _ := s.GetPod("default", "p")
	got.Status.Phase = v1.PodFailed

	fresh, _ := s.GetPod("default", "p")
	if fresh.Status.Phase == v1.PodFailed {
		t.Fatalf("store leaked internal reference; phase was mutated externally")
	}
}

func TestWatchReceivesEvents(t *testing.T) {
	s := New()

	w := s.Watch(ResourcePods, false)
	defer w.Stop()

	_, _ = s.CreatePod(&v1.Pod{ObjectMeta: v1.ObjectMeta{Name: "w1", Namespace: "default"}})

	select {
	case ev := <-w.ResultChan():
		if ev.Type != v1.WatchAdded {
			t.Fatalf("expected ADDED, got %s", ev.Type)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for watch event")
	}
}

func TestWatchInitialList(t *testing.T) {
	s := New()
	_, _ = s.CreatePod(&v1.Pod{ObjectMeta: v1.ObjectMeta{Name: "existing", Namespace: "default"}})

	w := s.Watch(ResourcePods, true)
	defer w.Stop()

	select {
	case ev := <-w.ResultChan():
		if ev.Type != v1.WatchAdded {
			t.Fatalf("expected initial ADDED, got %s", ev.Type)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for initial watch event")
	}
}

func TestNodeUpsertAndHeartbeat(t *testing.T) {
	s := New()
	_, err := s.UpsertNode(&v1.Node{ObjectMeta: v1.ObjectMeta{Name: "n1"}, Status: v1.NodeStatus{Phase: v1.NodeReady}})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	n, err := s.GetNode("n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if n.Status.Phase != v1.NodeReady {
		t.Fatalf("expected Ready, got %s", n.Status.Phase)
	}
	if n.Status.LastHeartbeat.IsZero() {
		t.Fatalf("expected heartbeat timestamp to be set")
	}
}

// Package kubelet  mini-k8s  Pod

//

//

//  1.  apiserver Node Ready

//  2.  Pod runtime //

//  3.  Pod  phase apiserver

//  4.  Pod  apiserver

//

//  list +  reconcile

//  kubelet  watch + PLEG  mini

package kubelet

import (
	"context"

	"fmt"

	"log"

	"strings"

	"time"

	v1 "github.com/sunpeng/mini-k8s/pkg/apis/core/v1"

	"github.com/sunpeng/mini-k8s/pkg/client"

	"github.com/sunpeng/mini-k8s/pkg/runtime"
)

// Kubelet  Pod

type Kubelet struct {
	nodeName string

	client *client.Client

	runtime *runtime.Runtime

	syncInterval time.Duration

	heartbeatInterval time.Duration
}

// New  kubelet

func New(nodeName string, c *client.Client, rt *runtime.Runtime, syncInterval, heartbeatInterval time.Duration) *Kubelet {

	if syncInterval <= 0 {

		syncInterval = 2 * time.Second

	}

	if heartbeatInterval <= 0 {

		heartbeatInterval = 5 * time.Second

	}

	return &Kubelet{

		nodeName: nodeName,

		client: c,

		runtime: rt,

		syncInterval: syncInterval,

		heartbeatInterval: heartbeatInterval,
	}

}

// Run  kubelet ctx

func (k *Kubelet) Run(ctx context.Context) {

	log.Printf("kubelet %q started (sync=%s, heartbeat=%s)", k.nodeName, k.syncInterval, k.heartbeatInterval)

	k.registerNode(ctx)

	heartbeat := time.NewTicker(k.heartbeatInterval)

	defer heartbeat.Stop()

	sync := time.NewTicker(k.syncInterval)

	defer sync.Stop()

	//

	k.syncOnce(ctx)

	for {

		select {

		case <-ctx.Done():

			log.Printf("kubelet %q shutting down, stopping all containers", k.nodeName)

			k.runtime.StopAll()

			return

		case <-heartbeat.C:

			k.registerNode(ctx)

		case <-sync.C:

			k.syncOnce(ctx)

		}

	}

}

func (k *Kubelet) registerNode(ctx context.Context) {

	node := &v1.Node{

		ObjectMeta: v1.ObjectMeta{Name: k.nodeName, Labels: map[string]string{"role": "worker"}},

		Status: v1.NodeStatus{Phase: v1.NodeReady},
	}

	if err := k.client.RegisterNode(ctx, node); err != nil {

		log.Printf("kubelet %q: register node failed: %v", k.nodeName, err)

	}

}

// syncOnce  reconcile

func (k *Kubelet) syncOnce(ctx context.Context) {

	pods, err := k.client.ListPods(ctx)

	if err != nil {

		log.Printf("kubelet %q: list pods failed: %v", k.nodeName, err)

		return

	}

	//  Pod

	desired := make(map[string]struct{})

	for i := range pods {

		p := pods[i]

		if p.Spec.NodeName != k.nodeName {

			continue

		}

		desired[p.UID] = struct{}{}

		k.syncPod(ctx, &p)

	}

	//  apiserver   runtime  Pod

	for _, uid := range k.runtime.PodUIDs() {

		if _, ok := desired[uid]; !ok {

			log.Printf("kubelet %q: pod uid=%s no longer exists, stopping containers", k.nodeName, uid)

			k.runtime.StopPod(uid)

		}

	}

}

// syncPod  Pod  status

func (k *Kubelet) syncPod(ctx context.Context, pod *v1.Pod) {

	if pod.UID == "" {

		return

	}

	policy := pod.Spec.RestartPolicy

	if policy == "" {

		policy = v1.RestartPolicyAlways

	}

	var (
		statuses []v1.ContainerStatus

		running bool

		allTerminated = len(pod.Spec.Containers) > 0

		anyFailed bool

		startErrs []string
	)

	for _, c := range pod.Spec.Containers {

		snap, ok := k.runtime.Snapshot(pod.UID, c.Name)

		switch {

		case !ok:

			//

			if _, err := k.runtime.StartContainer(pod.UID, c); err != nil {

				startErrs = append(startErrs, fmt.Sprintf("%s: %v", c.Name, err))

				statuses = append(statuses, v1.ContainerStatus{

					Name: c.Name,

					State: v1.ContainerWaiting,

					Reason: "StartError",
				})

				allTerminated = false

				continue

			}

			snap, ok = k.runtime.Snapshot(pod.UID, c.Name)

		case snap.State == v1.ContainerTerminated:

			//

			if shouldRestart(snap.ExitCode, policy) {

				if _, err := k.runtime.StartContainer(pod.UID, c); err != nil {

					startErrs = append(startErrs, fmt.Sprintf("%s restart: %v", c.Name, err))

				} else {

					snap, ok = k.runtime.Snapshot(pod.UID, c.Name)

				}

			}

		}

		if !ok || snap == nil {

			continue

		}

		cs := v1.ContainerStatus{

			Name: snap.Name,

			State: snap.State,

			StartedAt: snap.StartedAt,

			FinishedAt: snap.FinishedAt,

			ExitCode: snap.ExitCode,

			Reason: snap.Reason,

			RestartCount: snap.RestartCount,

			PID: snap.PID,
		}

		statuses = append(statuses, cs)

		switch snap.State {

		case v1.ContainerRunning:

			running = true

			allTerminated = false

		case v1.ContainerWaiting:

			allTerminated = false

		case v1.ContainerTerminated:

			if snap.ExitCode != 0 {

				anyFailed = true

			}

		}

	}

	//  Pod phase

	var phase v1.PodPhase

	switch {

	case allTerminated:

		if anyFailed {

			phase = v1.PodFailed

		} else {

			phase = v1.PodSucceeded

		}

	case running:

		phase = v1.PodRunning

	default:

		phase = v1.PodPending

	}

	newStatus := v1.PodStatus{

		Phase: phase,

		ContainerStatuses: statuses,

		StartTime: pod.Status.StartTime,
	}

	if len(startErrs) > 0 {

		newStatus.Message = strings.Join(startErrs, "; ")

	}

	if (phase == v1.PodRunning || phase == v1.PodSucceeded || phase == v1.PodFailed) && newStatus.StartTime == nil {

		now := time.Now()

		newStatus.StartTime = &now

	}

	newStatus.Conditions = buildConditions(phase, newStatus.StartTime)

	//   update

	if statusEqual(pod.Status, newStatus) {

		return

	}

	pod.Status = newStatus

	if _, err := k.client.UpdatePod(ctx, pod); err != nil {

		log.Printf("kubelet %q: update status for pod %s/%s failed: %v", k.nodeName, pod.Namespace, pod.Name, err)

		return

	}

	log.Printf("kubelet %q: pod %s/%s -> %s", k.nodeName, pod.Namespace, pod.Name, phase)

}

// shouldRestart

func shouldRestart(exitCode int32, policy v1.RestartPolicy) bool {

	switch policy {

	case v1.RestartPolicyAlways:

		return true

	case v1.RestartPolicyOnFailure:

		return exitCode != 0

	case v1.RestartPolicyNever:

		return false

	default:

		return true

	}

}

func buildConditions(phase v1.PodPhase, startTime *time.Time) []v1.PodCondition {

	now := time.Now()

	scheduled := v1.PodCondition{Type: "PodScheduled", Status: "True"}

	if startTime != nil {

		scheduled.Time = *startTime

	} else {

		scheduled.Time = now

	}

	ready := v1.PodCondition{Type: "Ready", Status: "False", Time: now}

	if phase == v1.PodRunning {

		ready.Status = "True"

	}

	return []v1.PodCondition{scheduled, ready}

}

// statusEqual  status

func statusEqual(a, b v1.PodStatus) bool {

	if a.Phase != b.Phase || a.Message != b.Message {

		return false

	}

	if len(a.ContainerStatuses) != len(b.ContainerStatuses) {

		return false

	}

	for i := range a.ContainerStatuses {

		x, y := a.ContainerStatuses[i], b.ContainerStatuses[i]

		if x.Name != y.Name || x.State != y.State || x.ExitCode != y.ExitCode ||

			x.RestartCount != y.RestartCount || x.PID != y.PID || x.Reason != y.Reason {

			return false

		}

		// FinishedAt

		if (x.FinishedAt == nil) != (y.FinishedAt == nil) {

			return false

		}

	}

	return true

}

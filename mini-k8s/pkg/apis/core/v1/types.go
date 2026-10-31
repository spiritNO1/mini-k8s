// Package v1  mini-k8s   API  Kubernetes core/v1
package v1

import "time"

// TypeMeta  API
type TypeMeta struct {
	Kind       string `json:"kind,omitempty"`
	APIVersion string `json:"apiVersion,omitempty"`
}

// ObjectMeta
type ObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	CreationTimestamp time.Time         `json:"creationTimestamp,omitempty"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp,omitempty"`
	ResourceVersion   int64             `json:"resourceVersion,omitempty"`
}

// PodPhase  Pod
type PodPhase string

const (
	PodPending   PodPhase = "Pending"   //  apiserver /
	PodRunning   PodPhase = "Running"   //
	PodSucceeded PodPhase = "Succeeded" //
	PodFailed    PodPhase = "Failed"    //
	PodUnknown   PodPhase = "Unknown"   //
)

// ContainerState
type ContainerState string

const (
	ContainerWaiting    ContainerState = "Waiting"
	ContainerRunning    ContainerState = "Running"
	ContainerTerminated ContainerState = "Terminated"
)

// RestartPolicy  Pod
type RestartPolicy string

const (
	RestartPolicyAlways    RestartPolicy = "Always"
	RestartPolicyOnFailure RestartPolicy = "OnFailure"
	RestartPolicyNever     RestartPolicy = "Never"
)

// EnvVar
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Container  Pod  mini-k8s
type Container struct {
	Name    string   `json:"name"`
	Image   string   `json:"image,omitempty"`
	Command []string `json:"command,omitempty"` //  docker  entrypoint
	Args    []string `json:"args,omitempty"`
	Env     []EnvVar `json:"env,omitempty"`
}

// PodSpec  Pod
type PodSpec struct {
	Containers    []Container   `json:"containers"`
	NodeName      string        `json:"nodeName,omitempty"`
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`
}

// ContainerStatus
type ContainerStatus struct {
	Name         string         `json:"name"`
	State        ContainerState `json:"state"`
	StartedAt    *time.Time     `json:"startedAt,omitempty"`
	FinishedAt   *time.Time     `json:"finishedAt,omitempty"`
	ExitCode     int32          `json:"exitCode"`
	Reason       string         `json:"reason,omitempty"`
	RestartCount int32          `json:"restartCount"`
	PID          int            `json:"pid,omitempty"`
}

// PodCondition  Pod
type PodCondition struct {
	Type   string    `json:"type"`
	Status string    `json:"status"` // "True" / "False" / "Unknown"
	Time   time.Time `json:"time,omitempty"`
}

// PodStatus  Pod
type PodStatus struct {
	Phase             PodPhase          `json:"phase"`
	Conditions        []PodCondition    `json:"conditions,omitempty"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty"`
	StartTime         *time.Time        `json:"startTime,omitempty"`
	Message           string            `json:"message,omitempty"`
}

// Pod  mini-k8s
type Pod struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Spec       PodSpec   `json:"spec"`
	Status     PodStatus `json:"status,omitempty"`
}

// NodePhase
type NodePhase string

const (
	NodeReady    NodePhase = "Ready"
	NodeNotReady NodePhase = "NotReady"
)

// NodeStatus
type NodeStatus struct {
	Phase         NodePhase `json:"phase"`
	LastHeartbeat time.Time `json:"lastHeartbeat,omitempty"`
}

// Node  kubelet
type Node struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Status     NodeStatus `json:"status,omitempty"`
}

// PodList  Pod
type PodList struct {
	TypeMeta `json:",inline"`
	Items    []Pod `json:"items"`
}

// NodeList  Node
type NodeList struct {
	TypeMeta `json:",inline"`
	Items    []Node `json:"items"`
}

// WatchEventType  watch
type WatchEventType string

const (
	WatchAdded    WatchEventType = "ADDED"
	WatchModified WatchEventType = "MODIFIED"
	WatchDeleted  WatchEventType = "DELETED"
)

// WatchEvent  watch
type WatchEvent struct {
	Type   WatchEventType `json:"type"`
	Object interface{}    `json:"object"`
}

// DeepCopy  Pod
func (p *Pod) DeepCopy() *Pod {
	if p == nil {
		return nil
	}
	out := *p
	// labels
	if p.Labels != nil {
		out.Labels = make(map[string]string, len(p.Labels))
		for k, v := range p.Labels {
			out.Labels[k] = v
		}
	}
	if p.Annotations != nil {
		out.Annotations = make(map[string]string, len(p.Annotations))
		for k, v := range p.Annotations {
			out.Annotations[k] = v
		}
	}
	// spec.containers
	if p.Spec.Containers != nil {
		out.Spec.Containers = make([]Container, len(p.Spec.Containers))
		for i, c := range p.Spec.Containers {
			cc := c
			if c.Command != nil {
				cc.Command = append([]string(nil), c.Command...)
			}
			if c.Args != nil {
				cc.Args = append([]string(nil), c.Args...)
			}
			if c.Env != nil {
				cc.Env = append([]EnvVar(nil), c.Env...)
			}
			out.Spec.Containers[i] = cc
		}
	}
	// status slices
	if p.Status.Conditions != nil {
		out.Status.Conditions = append([]PodCondition(nil), p.Status.Conditions...)
	}
	if p.Status.ContainerStatuses != nil {
		out.Status.ContainerStatuses = append([]ContainerStatus(nil), p.Status.ContainerStatuses...)
	}
	return &out
}

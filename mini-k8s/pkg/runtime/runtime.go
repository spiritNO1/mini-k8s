// Package runtime

//

//  Kubernetes kubelet  CRI  containerd/docker

//  mini-k8s

//  Container  Command stdout/stderr

package runtime

import (
	"context"

	"fmt"

	"os"

	"os/exec"

	"path/filepath"

	"sync"

	"time"

	v1 "github.com/sunpeng/mini-k8s/pkg/apis/core/v1"
)

// ContainerInfo

type ContainerInfo struct {
	Name string

	PID int

	State v1.ContainerState

	StartedAt *time.Time

	FinishedAt *time.Time

	ExitCode int32

	Reason string

	RestartCount int32

	cmd *exec.Cmd

	cancel context.CancelFunc

	logPath string

	done chan struct{} //

}

// Runtime

type Runtime struct {
	mu sync.Mutex

	logDir string

	// key: podUID/containerName

	procs map[string]*ContainerInfo
}

// New logDir

func New(logDir string) (*Runtime, error) {

	if err := os.MkdirAll(logDir, 0o755); err != nil {

		return nil, fmt.Errorf("create log dir: %w", err)

	}

	return &Runtime{

		logDir: logDir,

		procs: make(map[string]*ContainerInfo),
	}, nil

}

func key(podUID, container string) string { return podUID + "/" + container }

// StartContainer

func (r *Runtime) StartContainer(podUID string, c v1.Container) (*ContainerInfo, error) {

	r.mu.Lock()

	defer r.mu.Unlock()

	k := key(podUID, c.Name)

	//

	if info, ok := r.procs[k]; ok {

		if info.State == v1.ContainerRunning {

			return info, nil

		}

		//

		restartCount := info.RestartCount + 1

		info, err := r.startLocked(podUID, c, restartCount)

		return info, err

	}

	return r.startLocked(podUID, c, 0)

}

// startLocked

func (r *Runtime) startLocked(podUID string, c v1.Container, restartCount int32) (*ContainerInfo, error) {

	if len(c.Command) == 0 {

		return nil, fmt.Errorf("container %q has no command; process runtime requires a command", c.Name)

	}

	k := key(podUID, c.Name)

	logPath := filepath.Join(r.logDir, fmt.Sprintf("%s-%s.log", podUID, c.Name))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)

	if err != nil {

		return nil, fmt.Errorf("open log file: %w", err)

	}

	ctx, cancel := context.WithCancel(context.Background())

	binary := c.Command[0]

	args := append(append([]string(nil), c.Command[1:]...), c.Args...)

	cmd := exec.CommandContext(ctx, binary, args...)

	//

	cmd.Env = append(os.Environ(), fmt.Sprintf("MINI_K8S_POD_UID=%s", podUID))

	for _, e := range c.Env {

		cmd.Env = append(cmd.Env, e.Name+"="+e.Value)

	}

	cmd.Stdout = logFile

	cmd.Stderr = logFile

	now := time.Now()

	info := &ContainerInfo{

		Name: c.Name,

		State: v1.ContainerWaiting,

		RestartCount: restartCount,

		cmd: cmd,

		cancel: cancel,

		logPath: logPath,

		done: make(chan struct{}),
	}

	if err := cmd.Start(); err != nil {

		cancel()

		logFile.Close()

		return nil, fmt.Errorf("start container %q: %w", c.Name, err)

	}

	info.PID = cmd.Process.Pid

	info.State = v1.ContainerRunning

	startedAt := now

	info.StartedAt = &startedAt

	r.procs[k] = info

	//

	go func() {

		err := cmd.Wait()

		logFile.Close()

		finished := time.Now()

		r.mu.Lock()

		info.State = v1.ContainerTerminated

		info.FinishedAt = &finished

		if err != nil {

			if exitErr, ok := err.(*exec.ExitError); ok {

				info.ExitCode = int32(exitErr.ExitCode())

				info.Reason = "Error"

			} else {

				info.ExitCode = -1

				info.Reason = "ContainerStatusUnknown"

			}

		} else {

			info.ExitCode = 0

			info.Reason = "Completed"

		}

		r.mu.Unlock()

		close(info.done)

	}()

	return info, nil

}

// StopContainer  kill

func (r *Runtime) StopContainer(podUID, container string) {

	r.mu.Lock()

	defer r.mu.Unlock()

	k := key(podUID, container)

	info, ok := r.procs[k]

	if !ok {

		return

	}

	if info.State == v1.ContainerRunning && info.cancel != nil {

		info.cancel()

	}

}

// StopPod  Pod

func (r *Runtime) StopPod(podUID string) {

	r.mu.Lock()

	var toCancel []*ContainerInfo

	for k, info := range r.procs {

		if len(k) > len(podUID) && k[:len(podUID)+1] == podUID+"/" {

			if info.State == v1.ContainerRunning && info.cancel != nil {

				toCancel = append(toCancel, info)

			}

			delete(r.procs, k)

		}

	}

	r.mu.Unlock()

	for _, info := range toCancel {

		info.cancel()

	}

}

// Snapshot

func (r *Runtime) Snapshot(podUID, container string) (*ContainerInfo, bool) {

	r.mu.Lock()

	defer r.mu.Unlock()

	info, ok := r.procs[key(podUID, container)]

	if !ok {

		return nil, false

	}

	cp := *info

	cp.cmd = nil

	cp.cancel = nil

	return &cp, true

}

// IsRunning

func (r *Runtime) IsRunning(podUID, container string) bool {

	info, ok := r.Snapshot(podUID, container)

	return ok && info.State == v1.ContainerRunning

}

// HasPod  Pod

func (r *Runtime) HasPod(podUID string) bool {

	r.mu.Lock()

	defer r.mu.Unlock()

	for k := range r.procs {

		if len(k) > len(podUID) && k[:len(podUID)+1] == podUID+"/" {

			return true

		}

	}

	return false

}

// LogPath

func (r *Runtime) LogPath(podUID, container string) (string, bool) {

	r.mu.Lock()

	defer r.mu.Unlock()

	info, ok := r.procs[key(podUID, container)]

	if !ok {

		return "", false

	}

	return info.logPath, true

}

// PodUIDs  Pod UID

func (r *Runtime) PodUIDs() []string {

	r.mu.Lock()

	defer r.mu.Unlock()

	seen := make(map[string]struct{})

	for k := range r.procs {

		for i := 0; i < len(k); i++ {

			if k[i] == '/' {

				seen[k[:i]] = struct{}{}

				break

			}

		}

	}

	out := make([]string, 0, len(seen))

	for uid := range seen {

		out = append(out, uid)

	}

	return out

}

// StopAll  kubelet

func (r *Runtime) StopAll() {

	r.mu.Lock()

	var toCancel []*ContainerInfo

	for _, info := range r.procs {

		if info.State == v1.ContainerRunning && info.cancel != nil {

			toCancel = append(toCancel, info)

		}

	}

	r.procs = make(map[string]*ContainerInfo)

	r.mu.Unlock()

	for _, info := range toCancel {

		info.cancel()

	}

}

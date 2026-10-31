// Command minictl  mini-k8s  kubectl

//

//

//

//minictl get pods                  Pod

//minictl get nodes

//minictl run NAME -- CMD ARGS...   Pod

//minictl apply -f manifest.json    JSON  Pod

//minictl describe pod NAME         Pod

//minictl delete pod NAME            Pod

//minictl logs NAME [-c CONTAINER]

//

// apiserver  --server  MINI_K8S_SERVER  http://127.0.0.1:8080

package main

import (
	"context"

	"encoding/json"

	"flag"

	"fmt"

	"os"

	"path/filepath"

	"strings"

	"text/tabwriter"

	"time"

	v1 "github.com/sunpeng/mini-k8s/pkg/apis/core/v1"

	"github.com/sunpeng/mini-k8s/pkg/client"
)

func main() {

	if len(os.Args) < 2 {

		usage()

		os.Exit(2)

	}

	server := envOr("MINI_K8S_SERVER", "http://127.0.0.1:8080")

	//  --server

	args := os.Args[1:]

	args = extractServerFlag(args, &server)

	c := client.New(server)

	ctx := context.Background()

	cmd := args[0]

	rest := args[1:]

	var err error

	switch cmd {

	case "get":

		err = cmdGet(ctx, c, rest)

	case "run":

		err = cmdRun(ctx, c, rest)

	case "apply":

		err = cmdApply(ctx, c, rest)

	case "describe":

		err = cmdDescribe(ctx, c, rest)

	case "delete":

		err = cmdDelete(ctx, c, rest)

	case "logs":

		err = cmdLogs(ctx, c, rest)

	case "help", "-h", "--help":

		usage()

		return

	default:

		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)

		usage()

		os.Exit(2)

	}

	if err != nil {

		fmt.Fprintf(os.Stderr, "error: %v\n", err)

		os.Exit(1)

	}

}

// ------------------------- get -------------------------

func cmdGet(ctx context.Context, c *client.Client, args []string) error {

	resource := "pods"

	if len(args) > 0 {

		resource = normalizeResource(args[0])

		args = args[1:]

	}

	switch resource {

	case "pods":

		return getPods(ctx, c, args)

	case "nodes":

		return getNodes(ctx, c)

	default:

		return fmt.Errorf("unsupported resource %q (supported: pods, nodes)", resource)

	}

}

func getPods(ctx context.Context, c *client.Client, args []string) error {

	pods, err := c.ListPods(ctx)

	if err != nil {

		return err

	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 3, ' ', 0)

	fmt.Fprintln(w, "NAME\tNAMESPACE\tPHASE\tNODE\tRESTARTS\tAGE")

	for _, p := range pods {

		if len(args) > 0 && p.Name != args[0] {

			continue

		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\n",

			p.Name, p.Namespace, p.Status.Phase, orDash(p.Spec.NodeName),

			totalRestarts(p), age(p.CreationTimestamp))

	}

	return w.Flush()

}

func getNodes(ctx context.Context, c *client.Client) error {

	nodes, err := c.ListNodes(ctx)

	if err != nil {

		return err

	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 3, ' ', 0)

	fmt.Fprintln(w, "NAME\tSTATUS\tLAST_HEARTBEAT")

	for _, n := range nodes {

		hb := "-"

		if !n.Status.LastHeartbeat.IsZero() {

			hb = n.Status.LastHeartbeat.Local().Format(time.RFC3339)

		}

		fmt.Fprintf(w, "%s\t%s\t%s\n", n.Name, n.Status.Phase, hb)

	}

	return w.Flush()

}

// ------------------------- run -------------------------

func cmdRun(ctx context.Context, c *client.Client, args []string) error {

	if len(args) < 1 {

		return fmt.Errorf("usage: minictl run NAME [--image IMG] [--env K=V]... -- COMMAND [ARGS...]")

	}

	// name  flag  `--`

	name := args[0]

	rest := args[1:]

	var flagPart, command []string

	if idx := indexOf(rest, "--"); idx >= 0 {

		flagPart = rest[:idx]

		command = rest[idx+1:]

	} else {

		flagPart = rest

	}

	fs := flag.NewFlagSet("run", flag.ExitOnError)

	image := fs.String("image", "", " ")

	namespace := fs.String("n", "default", "")

	restart := fs.String("restart", "Always", "Always|OnFailure|Never")

	var envs multiFlag

	fs.Var(&envs, "env", " K=V")

	fs.Parse(flagPart)

	if len(command) == 0 {

		return fmt.Errorf("a command is required after '--'")

	}

	pod := &v1.Pod{

		ObjectMeta: v1.ObjectMeta{Name: name, Namespace: *namespace},

		Spec: v1.PodSpec{

			RestartPolicy: v1.RestartPolicy(*restart),

			Containers: []v1.Container{{

				Name: name,

				Image: *image,

				Command: command,

				Env: envs.toEnvVars(),
			}},
		},
	}

	created, err := c.CreatePod(ctx, pod)

	if err != nil {

		return err

	}

	fmt.Printf("pod/%s created (uid=%s)\n", created.Name, created.UID)

	return nil

}

// ------------------------- apply -------------------------

func cmdApply(ctx context.Context, c *client.Client, args []string) error {

	fs := flag.NewFlagSet("apply", flag.ExitOnError)

	file := fs.String("f", "", "Pod JSON ")

	fs.Parse(args)

	if *file == "" {

		return fmt.Errorf("usage: minictl apply -f manifest.json")

	}

	data, err := os.ReadFile(*file)

	if err != nil {

		return err

	}

	var pod v1.Pod

	if err := json.Unmarshal(data, &pod); err != nil {

		return fmt.Errorf("parse manifest: %w", err)

	}

	if pod.Name == "" {

		return fmt.Errorf("manifest missing metadata.name")

	}

	created, err := c.CreatePod(ctx, &pod)

	if err != nil {

		return err

	}

	fmt.Printf("pod/%s created\n", created.Name)

	return nil

}

// ------------------------- describe -------------------------

func cmdDescribe(ctx context.Context, c *client.Client, args []string) error {

	if len(args) < 2 || normalizeResource(args[0]) != "pods" {

		return fmt.Errorf("usage: minictl describe pod NAME")

	}

	name := args[1]

	fs := flag.NewFlagSet("describe", flag.ExitOnError)

	namespace := fs.String("n", "default", "")

	fs.Parse(args[2:])

	pod, err := c.GetPod(ctx, *namespace, name)

	if err != nil {

		return err

	}

	fmt.Printf("Name:         %s\n", pod.Name)

	fmt.Printf("Namespace:    %s\n", pod.Namespace)

	fmt.Printf("UID:          %s\n", pod.UID)

	fmt.Printf("Node:         %s\n", orDash(pod.Spec.NodeName))

	fmt.Printf("CreationTime: %s\n", pod.CreationTimestamp.Local().Format(time.RFC3339))

	fmt.Printf("Phase:        %s\n", pod.Status.Phase)

	if pod.Status.Message != "" {

		fmt.Printf("Message:      %s\n", pod.Status.Message)

	}

	if pod.Status.StartTime != nil {

		fmt.Printf("StartTime:    %s\n", pod.Status.StartTime.Local().Format(time.RFC3339))

	}

	fmt.Printf("RestartPolicy:%s\n", orDefault(string(pod.Spec.RestartPolicy), "Always"))

	if len(pod.Status.Conditions) > 0 {

		fmt.Println("Conditions:")

		for _, cond := range pod.Status.Conditions {

			fmt.Printf("  Type=%s Status=%s\n", cond.Type, cond.Status)

		}

	}

	fmt.Println("Containers:")

	statusByName := map[string]v1.ContainerStatus{}

	for _, cs := range pod.Status.ContainerStatuses {

		statusByName[cs.Name] = cs

	}

	for _, ct := range pod.Spec.Containers {

		fmt.Printf("  %s:\n", ct.Name)

		if ct.Image != "" {

			fmt.Printf("    Image:   %s\n", ct.Image)

		}

		fmt.Printf("    Command: %s\n", strings.Join(append(ct.Command, ct.Args...), " "))

		if len(ct.Env) > 0 {

			for _, e := range ct.Env {

				fmt.Printf("    Env:     %s=%s\n", e.Name, e.Value)

			}

		}

		if cs, ok := statusByName[ct.Name]; ok {

			fmt.Printf("    State:   %s (exit=%d, restarts=%d, pid=%d)\n",

				cs.State, cs.ExitCode, cs.RestartCount, cs.PID)

			if cs.Reason != "" {

				fmt.Printf("    Reason:  %s\n", cs.Reason)

			}

			if cs.StartedAt != nil {

				fmt.Printf("    Started: %s\n", cs.StartedAt.Local().Format(time.RFC3339))

			}

			if cs.FinishedAt != nil {

				fmt.Printf("    Ended:   %s\n", cs.FinishedAt.Local().Format(time.RFC3339))

			}

		} else {

			fmt.Printf("    State:   Waiting\n")

		}

	}

	return nil

}

// ------------------------- delete -------------------------

func cmdDelete(ctx context.Context, c *client.Client, args []string) error {

	if len(args) < 2 || normalizeResource(args[0]) != "pods" {

		return fmt.Errorf("usage: minictl delete pod NAME")

	}

	name := args[1]

	fs := flag.NewFlagSet("delete", flag.ExitOnError)

	namespace := fs.String("n", "default", "")

	fs.Parse(args[2:])

	if err := c.DeletePod(ctx, *namespace, name); err != nil {

		return err

	}

	fmt.Printf("pod/%s deleted\n", name)

	return nil

}

// ------------------------- logs -------------------------

func cmdLogs(ctx context.Context, c *client.Client, args []string) error {

	if len(args) < 1 {

		return fmt.Errorf("usage: minictl logs NAME [-c CONTAINER] [--logdir DIR]")

	}

	// name  flag

	name := args[0]

	fs := flag.NewFlagSet("logs", flag.ExitOnError)

	container := fs.String("c", "", "")

	namespace := fs.String("n", "default", "")

	logDir := fs.String("logdir", envOr("MINI_K8S_LOG_DIR", "/tmp/mini-k8s/logs"), "kubelet ")

	fs.Parse(args[1:])

	pod, err := c.GetPod(ctx, *namespace, name)

	if err != nil {

		return err

	}

	ct := *container

	if ct == "" {

		if len(pod.Spec.Containers) == 0 {

			return fmt.Errorf("pod %s has no containers", name)

		}

		ct = pod.Spec.Containers[0].Name

	}

	logPath := filepath.Join(*logDir, fmt.Sprintf("%s-%s.log", pod.UID, ct))

	data, err := os.ReadFile(logPath)

	if err != nil {

		return fmt.Errorf("read logs %s: %w", logPath, err)

	}

	os.Stdout.Write(data)

	return nil

}

// ------------------------- helpers -------------------------

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func (m *multiFlag) toEnvVars() []v1.EnvVar {

	out := make([]v1.EnvVar, 0, len(*m))

	for _, kv := range *m {

		parts := strings.SplitN(kv, "=", 2)

		if len(parts) == 2 {

			out = append(out, v1.EnvVar{Name: parts[0], Value: parts[1]})

		}

	}

	return out

}

func extractServerFlag(args []string, server *string) []string {

	out := make([]string, 0, len(args))

	for i := 0; i < len(args); i++ {

		switch {

		case args[i] == "--server" && i+1 < len(args):

			*server = args[i+1]

			i++

		case strings.HasPrefix(args[i], "--server="):

			*server = strings.TrimPrefix(args[i], "--server=")

		default:

			out = append(out, args[i])

		}

	}

	return out

}

// indexOf  target  s   -1

func indexOf(s []string, target string) int {

	for i, v := range s {

		if v == target {

			return i

		}

	}

	return -1

}

func normalizeResource(r string) string {

	switch strings.ToLower(r) {

	case "pod", "pods", "po":

		return "pods"

	case "node", "nodes", "no":

		return "nodes"

	default:

		return strings.ToLower(r)

	}

}

func totalRestarts(p v1.Pod) int32 {

	var n int32

	for _, cs := range p.Status.ContainerStatuses {

		n += cs.RestartCount

	}

	return n

}

func age(t time.Time) string {

	if t.IsZero() {

		return "-"

	}

	d := time.Since(t)

	switch {

	case d < time.Minute:

		return fmt.Sprintf("%ds", int(d.Seconds()))

	case d < time.Hour:

		return fmt.Sprintf("%dm", int(d.Minutes()))

	default:

		return fmt.Sprintf("%dh", int(d.Hours()))

	}

}

func orDash(s string) string {

	if s == "" {

		return "<none>"

	}

	return s

}

func orDefault(s, def string) string {

	if s == "" {

		return def

	}

	return s

}

func envOr(key, def string) string {

	if v := os.Getenv(key); v != "" {

		return v

	}

	return def

}

func usage() {

	fmt.Print(`minictl - mini-k8s 

 

:

  minictl [--server URL] <command> [args]

 

:

  get pods [NAME]               Pod

  get nodes                    

  run NAME [flags] -- CMD ...   Pod

                               flags: --image --env K=V --restart Always|OnFailure|Never -n NS

  apply -f manifest.json        JSON  Pod

  describe pod NAME             Pod 

  delete pod NAME                Pod

  logs NAME [-c CONTAINER]     --logdir 

 

:

  --server URL                 apiserver  http://127.0.0.1:8080 MINI_K8S_SERVER

`)

}

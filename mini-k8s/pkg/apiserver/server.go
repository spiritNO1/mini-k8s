// Package apiserver  mini-k8s  HTTP REST

//  Pod/Node  CRUD  Watch  store.Store

package apiserver

import (
	"crypto/rand"

	"encoding/hex"

	"encoding/json"

	"log"

	"net/http"

	"time"

	v1 "github.com/sunpeng/mini-k8s/pkg/apis/core/v1"

	"github.com/sunpeng/mini-k8s/pkg/store"
)

// Server  apiserver  HTTP

type Server struct {
	Store *store.Store

	mux *http.ServeMux
}

// New  apiserver

func New(s *store.Store) *Server {

	srv := &Server{Store: s, mux: http.NewServeMux()}

	srv.routes()

	return srv

}

// Handler  http.Server

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {

	s.mux.HandleFunc("GET /healthz", s.handleHealth)

	// Pods

	s.mux.HandleFunc("POST /api/v1/pods", s.handleCreatePod)

	s.mux.HandleFunc("GET /api/v1/pods", s.handleListPods)

	s.mux.HandleFunc("GET /api/v1/namespaces/{namespace}/pods/{name}", s.handleGetPod)

	s.mux.HandleFunc("PUT /api/v1/namespaces/{namespace}/pods/{name}", s.handleUpdatePod)

	s.mux.HandleFunc("DELETE /api/v1/namespaces/{namespace}/pods/{name}", s.handleDeletePod)

	s.mux.HandleFunc("GET /api/v1/watch/pods", s.handleWatchPods)

	// Nodes

	s.mux.HandleFunc("POST /api/v1/nodes", s.handleUpsertNode)

	s.mux.HandleFunc("GET /api/v1/nodes", s.handleListNodes)

	s.mux.HandleFunc("GET /api/v1/watch/nodes", s.handleWatchNodes)

}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})

}

// ------------------------- Pod handlers -------------------------

func (s *Server) handleCreatePod(w http.ResponseWriter, r *http.Request) {

	var pod v1.Pod

	if err := json.NewDecoder(r.Body).Decode(&pod); err != nil {

		writeError(w, http.StatusBadRequest, "invalid pod json: "+err.Error())

		return

	}

	if pod.Name == "" {

		writeError(w, http.StatusBadRequest, "pod name is required")

		return

	}

	if pod.Namespace == "" {

		pod.Namespace = "default"

	}

	if pod.UID == "" {

		pod.UID = newUID()

	}

	pod.Kind = "Pod"

	pod.APIVersion = "v1"

	created, err := s.Store.CreatePod(&pod)

	if err != nil {

		writeError(w, http.StatusConflict, err.Error())

		return

	}

	writeJSON(w, http.StatusCreated, created)

}

func (s *Server) handleListPods(w http.ResponseWriter, r *http.Request) {

	pods := s.Store.ListPods()

	list := v1.PodList{

		TypeMeta: v1.TypeMeta{Kind: "PodList", APIVersion: "v1"},

		Items: make([]v1.Pod, 0, len(pods)),
	}

	for _, p := range pods {

		list.Items = append(list.Items, *p)

	}

	writeJSON(w, http.StatusOK, list)

}

func (s *Server) handleGetPod(w http.ResponseWriter, r *http.Request) {

	ns := r.PathValue("namespace")

	name := r.PathValue("name")

	pod, err := s.Store.GetPod(ns, name)

	if err != nil {

		writeError(w, http.StatusNotFound, err.Error())

		return

	}

	writeJSON(w, http.StatusOK, pod)

}

func (s *Server) handleUpdatePod(w http.ResponseWriter, r *http.Request) {

	ns := r.PathValue("namespace")

	name := r.PathValue("name")

	var incoming v1.Pod

	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {

		writeError(w, http.StatusBadRequest, "invalid pod json: "+err.Error())

		return

	}

	//  store

	existing, err := s.Store.GetPod(ns, name)

	if err != nil {

		writeError(w, http.StatusNotFound, err.Error())

		return

	}

	existing.Spec = incoming.Spec

	existing.Status = incoming.Status

	if incoming.Labels != nil {

		existing.Labels = incoming.Labels

	}

	if incoming.Annotations != nil {

		existing.Annotations = incoming.Annotations

	}

	updated, err := s.Store.UpdatePod(existing)

	if err != nil {

		writeError(w, http.StatusInternalServerError, err.Error())

		return

	}

	writeJSON(w, http.StatusOK, updated)

}

func (s *Server) handleDeletePod(w http.ResponseWriter, r *http.Request) {

	ns := r.PathValue("namespace")

	name := r.PathValue("name")

	pod, err := s.Store.DeletePod(ns, name)

	if err != nil {

		writeError(w, http.StatusNotFound, err.Error())

		return

	}

	writeJSON(w, http.StatusOK, pod)

}

func (s *Server) handleWatchPods(w http.ResponseWriter, r *http.Request) {

	s.streamWatch(w, r, store.ResourcePods)

}

// ------------------------- Node handlers -------------------------

func (s *Server) handleUpsertNode(w http.ResponseWriter, r *http.Request) {

	var node v1.Node

	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {

		writeError(w, http.StatusBadRequest, "invalid node json: "+err.Error())

		return

	}

	if node.Name == "" {

		writeError(w, http.StatusBadRequest, "node name is required")

		return

	}

	node.Kind = "Node"

	node.APIVersion = "v1"

	saved, err := s.Store.UpsertNode(&node)

	if err != nil {

		writeError(w, http.StatusInternalServerError, err.Error())

		return

	}

	writeJSON(w, http.StatusOK, saved)

}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {

	nodes := s.Store.ListNodes()

	list := v1.NodeList{

		TypeMeta: v1.TypeMeta{Kind: "NodeList", APIVersion: "v1"},

		Items: make([]v1.Node, 0, len(nodes)),
	}

	for _, n := range nodes {

		list.Items = append(list.Items, *n)

	}

	writeJSON(w, http.StatusOK, list)

}

func (s *Server) handleWatchNodes(w http.ResponseWriter, r *http.Request) {

	s.streamWatch(w, r, store.ResourceNodes)

}

// ------------------------- Watch streaming -------------------------

// streamWatch  JSONndjson watch

func (s *Server) streamWatch(w http.ResponseWriter, r *http.Request, res store.Resource) {

	flusher, ok := w.(http.Flusher)

	if !ok {

		writeError(w, http.StatusInternalServerError, "streaming unsupported")

		return

	}

	w.Header().Set("Content-Type", "application/x-ndjson")

	w.Header().Set("Cache-Control", "no-cache")

	w.Header().Set("Connection", "keep-alive")

	w.WriteHeader(http.StatusOK)

	flusher.Flush()

	// sendInitial=true ADDED

	//   watch

	wt := s.Store.Watch(res, true)

	defer wt.Stop()

	enc := json.NewEncoder(w)

	ctx := r.Context()

	//

	ticker := time.NewTicker(20 * time.Second)

	defer ticker.Stop()

	for {

		select {

		case <-ctx.Done():

			return

		case ev, ok := <-wt.ResultChan():

			if !ok {

				return

			}

			if err := enc.Encode(ev); err != nil {

				return

			}

			flusher.Flush()

		case <-ticker.C:

			// ndjson

			if _, err := w.Write([]byte("\n")); err != nil {

				return

			}

			flusher.Flush()

		}

	}

}

// ------------------------- helpers -------------------------

func writeJSON(w http.ResponseWriter, status int, obj interface{}) {

	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(obj)

}

func writeError(w http.ResponseWriter, status int, msg string) {

	log.Printf("apiserver: %d %s", status, msg)

	writeJSON(w, status, map[string]interface{}{

		"kind": "Status",

		"status": "Failure",

		"message": msg,

		"code": status,
	})

}

// newUID  16  UID

func newUID() string {

	b := make([]byte, 16)

	if _, err := rand.Read(b); err != nil {

		return time.Now().UTC().Format("20060102150405.000000000")

	}

	return hex.EncodeToString(b)

}

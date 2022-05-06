package server

import (
	"encoding/json"
	"fmt"
	api "github.com/NathanClassen/hydralog/api/v1"
	hydralog "github.com/NathanClassen/hydralog/internal/log"
	"log"
	"net/http"
)

type httpServer struct {
	Log *hydralog.Log
}

// structs for request payloads and responses

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record api.Record `json:"record"`
}

func NewHTTPServer(addr string) *http.Server {
	// modified lines 31 and the handler section to use net/http std lib instead of gorilla
	httpserver := newHTTPServer()
	// using go stdlib instead of Gorilla as done in the book
	r := http.NewServeMux()

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			httpserver.handleProduce(w, r)
		case http.MethodGet:
			httpserver.handleConsume(w, r)
		}
	})

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

func newHTTPServer() *httpServer {
	c := hydralog.Config{}
	l, err := hydralog.NewLog("../test", c)
	if err != nil {
		log.Fatalf("error creating log: %v", err)
		return nil
	}
	return &httpServer{Log: l}
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	// a struct to hold the request body
	var req ProduceRequest

	// marshall req into struct
	err := json.NewDecoder(r.Body).Decode(&req)
	fmt.Println("req: ", req.Record.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// here we need to create an api.Record struct out of the parsed body
	toAppend := &api.Record{
		Value: req.Record.Value,
	}

	fmt.Println("toAppend", toAppend)
	fmt.Println("toAppend.Value", toAppend.Value)

	off, err := s.Log.Append(toAppend)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return

	}
	res := ConsumeResponse{Record: *record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

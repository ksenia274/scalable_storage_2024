package main

import (
	"net/http"
)

type Router struct {
	mux   *http.ServeMux
	nodes [][]string
}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
	result := Router{
		mux:   mux,
		nodes: nodes,
	}

	mux.Handle("/", http.FileServer(http.Dir("./front/dist")))

	mux.Handle("/select", http.RedirectHandler("/"+nodes[0][0]+"/select", http.StatusTemporaryRedirect))
	mux.Handle("/insert", http.RedirectHandler("/"+nodes[0][0]+"/insert", http.StatusTemporaryRedirect))
	mux.Handle("/replace", http.RedirectHandler("/"+nodes[0][0]+"/replace", http.StatusTemporaryRedirect))
	mux.Handle("/delete", http.RedirectHandler("/"+nodes[0][0]+"/delete", http.StatusTemporaryRedirect))
	return &result
}

func (r *Router) Run() {
	// Ничего не делаем
}

func (r *Router) Stop() {
	// Ничего не делаем
}

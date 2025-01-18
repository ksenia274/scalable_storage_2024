package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	mux := http.NewServeMux()

	storage := NewStorage(mux, "storage", []string{}, true)
	go func() { storage.Run() }()

	router := NewRouter(mux, [][]string{{"storage"}})
	go func() { router.Run() }()

	l := http.Server{}
	l.Addr = "127.0.0.1:8080"
	l.Handler = mux

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		slog.Info("got", "signal", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		l.Shutdown(ctx)
	}()

	defer func() {
		router.Stop()
		storage.Stop()
		slog.Info("we are going down")
	}()

	slog.Info("listen http://" + l.Addr)
	err := l.ListenAndServe() // http event loop
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}
}

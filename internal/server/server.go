package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/s3"
	"github.com/kajdesk/objex/internal/storage/local"
)

type Server struct {
	http  *http.Server
	store *local.Store
}

func New(cfg config.Config, logger *slog.Logger) (*Server, error) {
	store, err := local.Open(cfg.DataDir, cfg.Fsync)
	if err != nil {
		return nil, err
	}
	handler := s3.New(store, cfg)
	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           accessLog(logger, handler),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       30 * time.Second,
		WriteTimeout:      0, // large downloads may legitimately run for a long time
		MaxHeaderBytes:    1 << 20,
	}
	return &Server{http: httpServer, store: store}, nil
}

func (s *Server) Run(ctx context.Context, maxConnections int) error {
	listener, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return err
	}
	listener = limitListener(listener, maxConnections)
	done := make(chan error, 1)
	go func() { done <- s.http.Serve(listener) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = s.http.Shutdown(shutdownCtx)
		err := <-done
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) Close() error { return s.store.Close() }

func accessLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		logger.Info("request", "method", r.Method, "path", r.URL.Path, "status", wrapped.status, "duration", time.Since(start))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

type limitedListener struct {
	net.Listener
	sem chan struct{}
}

func limitListener(listener net.Listener, maximum int) net.Listener {
	if maximum < 1 {
		maximum = 1
	}
	return &limitedListener{Listener: listener, sem: make(chan struct{}, maximum)}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	l.sem <- struct{}{}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitedConn{Conn: conn, release: func() { <-l.sem }}, nil
}

type limitedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

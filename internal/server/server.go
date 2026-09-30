// Package server runs the HTTP server and background maintenance.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/kajdesk/objex/internal/auth"
	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/s3"
	"github.com/kajdesk/objex/internal/storage/local"
)

const (
	headerTimeout   = 30 * time.Second
	idleTimeout     = 60 * time.Second
	bodyIdleTimeout = 60 * time.Second
	shutdownTimeout = 30 * time.Second
	keyReloadEvery  = 2 * time.Second
	gcGrace         = time.Hour
)

type Server struct {
	cfg        config.Config
	configPath string
	store      *local.Store
	verifier   *auth.Verifier
	http       *http.Server
	log        *slog.Logger
}

func New(cfg config.Config, configPath string, logger *slog.Logger) (*Server, error) {
	store, err := local.Open(cfg.DataDir, local.Options{Fsync: cfg.Fsync, VerifyReads: cfg.VerifyReads})
	if err != nil {
		return nil, err
	}
	verifier := auth.New(cfg.Keys)
	handler := s3.New(store, verifier, s3.Options{Region: cfg.Region, Domain: cfg.Domain})
	s := &Server{cfg: cfg, configPath: configPath, store: store, verifier: verifier, log: logger}
	s.http = &http.Server{
		Addr:              cfg.Listen,
		Handler:           s.middleware(handler),
		ReadHeaderTimeout: headerTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelDebug),
	}
	return s, nil
}

// Handler returns the HTTP handler (for tests).
func (s *Server) Handler() http.Handler { return s.http.Handler }

// Store returns the storage engine.
func (s *Server) Store() *local.Store { return s.store }

func (s *Server) Close() error { return s.store.Close() }

// Run serves until ctx is cancelled, then drains in-flight requests.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	ln = limitListener(ln, s.cfg.MaxConnections)
	bg, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); s.reloadKeys(bg) }()
	go func() { defer wg.Done(); s.every(bg, s.cfg.GCIntervalHours, "gc", s.gc) }()
	go func() { defer wg.Done(); s.every(bg, s.cfg.ScrubIntervalHours, "scrub", s.scrub) }()
	defer func() { stop(); wg.Wait() }()

	done := make(chan error, 1)
	go func() { done <- s.http.Serve(ln) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := s.http.Shutdown(sctx); err != nil {
			s.log.Warn("shutdown timed out; closing remaining connections", "err", err)
			_ = s.http.Close()
		}
		<-done
		return nil
	}
}

// reloadKeys re-reads access keys whenever the config file changes.
func (s *Server) reloadKeys(ctx context.Context) {
	if s.configPath == "" {
		return
	}
	mtime := func() time.Time {
		st, err := os.Stat(s.configPath)
		if err != nil {
			return time.Time{}
		}
		return st.ModTime()
	}
	last := mtime()
	t := time.NewTicker(keyReloadEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if m := mtime(); !m.Equal(last) {
			last = m
			cfg, err := config.Load(s.configPath)
			if err != nil {
				s.log.Warn("not reloading keys", "err", err)
				continue
			}
			s.verifier.SetKeys(cfg.Keys)
			s.log.Info("reloaded access keys", "count", len(cfg.Keys))
		}
	}
}

func (s *Server) every(ctx context.Context, hours int, name string, job func()) {
	if hours <= 0 {
		return
	}
	t := time.NewTicker(time.Duration(hours) * time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			job()
		}
	}
}

func (s *Server) gc() {
	n, err := s.store.GC(gcGrace)
	switch {
	case err != nil:
		s.log.Warn("gc failed", "err", err)
	case n > 0:
		s.log.Info("gc removed orphaned blobs", "count", n)
	}
}

func (s *Server) scrub() {
	start := time.Now()
	r, err := s.store.Scrub(int64(s.cfg.ScrubMBPerSec) << 20)
	switch {
	case err != nil:
		s.log.Warn("scrub failed", "err", err)
	case len(r.Problems) > 0:
		s.log.Error("scrub found damaged blobs", "damaged", len(r.Problems), "checked", r.Blobs)
	default:
		s.log.Info("scrub verified all blobs", "blobs", r.Blobs, "bytes", r.Bytes, "took", time.Since(start).Round(time.Second))
	}
}

// middleware adds the access log and an idle timeout on request bodies, so a
// stalled upload cannot hold a connection forever.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		rc := http.NewResponseController(w)
		if r.Body != nil && r.Body != http.NoBody {
			r.Body = &idleBody{ReadCloser: r.Body, rc: rc}
		}
		next.ServeHTTP(sw, r)
		if r.URL.Path != s3.HealthPath {
			s.log.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.status, "bytes", sw.bytes, "ms", time.Since(start).Milliseconds(), "remote", r.RemoteAddr)
		}
	})
}

type idleBody struct {
	io.ReadCloser
	rc *http.ResponseController
}

func (b *idleBody) Read(p []byte) (int, error) {
	_ = b.rc.SetReadDeadline(time.Now().Add(bodyIdleTimeout))
	return b.ReadCloser.Read(p)
}

// statusWriter records the status for the access log. It forwards ReadFrom so
// net/http can still send files with sendfile.
type statusWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	w.wroteHeader = true
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *statusWriter) ReadFrom(r io.Reader) (int64, error) {
	w.wroteHeader = true
	var n int64
	var err error
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err = rf.ReadFrom(r)
	} else {
		n, err = io.Copy(w.ResponseWriter, r)
	}
	w.bytes += n
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type limitedListener struct {
	net.Listener
	sem chan struct{}
}

func limitListener(l net.Listener, n int) net.Listener {
	return &limitedListener{Listener: l, sem: make(chan struct{}, max(n, 1))}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	l.sem <- struct{}{}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitedConn{Conn: c, release: func() { <-l.sem }}, nil
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

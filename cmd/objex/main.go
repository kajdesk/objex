package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "objex:", err)
		os.Exit(1)
	}
}

func run() error {
	args := os.Args[1:]
	if len(args) > 0 {
		switch args[0] {
		case "health":
			return health(args[1:])
		case "server":
			args = args[1:]
		}
	}
	flags := flag.NewFlagSet("objex", flag.ContinueOnError)
	configPath := flags.String("config", envOr("OBJEX_CONFIG", "objex.json"), "JSON configuration file")
	listen := flags.String("listen", "", "listen address override")
	data := flags.String("data", "", "data directory override")
	noFsync := flags.Bool("no-fsync", false, "disable durable file and metadata sync")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *data != "" {
		cfg.DataDir = *data
	}
	if *noFsync {
		cfg.Fsync = false
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	app, err := server.New(cfg, logger)
	if err != nil {
		return err
	}
	defer app.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("objex listening", "address", cfg.Listen, "data", cfg.DataDir, "fsync", cfg.Fsync)
	return app.Run(ctx, cfg.MaxConnections)
}

func health(args []string) error {
	endpoint := "http://127.0.0.1:9000/_objex/health"
	if len(args) > 1 {
		return fmt.Errorf("usage: objex health [URL]")
	}
	if len(args) == 1 {
		endpoint = args[0]
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(endpoint)
	if err != nil {
		return fmt.Errorf("health request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("health request returned %s: %s", response.Status, body)
	}
	_, err = io.Copy(os.Stdout, response.Body)
	return err
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// Command objex is an S3 / R2 compatible object storage server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/server"
	"github.com/kajdesk/objex/internal/storage/local"
)

const usage = `usage: objex <command> [flags]

commands:
  server                 run the S3 server (default)
  init                   write a default config file
  key add <name>         generate an access key [-bucket b]... [-read-only]
  key list               list access keys
  key rm <key-or-name>   remove an access key
  scrub                  verify every stored blob (server must be stopped)
  health [URL]           check that a server is up (for healthchecks)

Common flag: -config (default objex.json, or $OBJEX_CONFIG)`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "objex:", err)
		os.Exit(1)
	}
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func run(args []string) error {
	cmd := "server"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet("objex "+cmd, flag.ContinueOnError)
	configPath := fs.String("config", envOr("OBJEX_CONFIG", "objex.json"), "JSON configuration file")
	switch cmd {
	case "server":
		listen := fs.String("listen", "", "listen address override")
		data := fs.String("data", "", "data directory override")
		noFsync := fs.Bool("no-fsync", false, "acknowledge writes without fsync (faster, not crash-safe)")
		if err := fs.Parse(args); err != nil {
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
		return serve(cfg, *configPath)
	case "init":
		if err := fs.Parse(args); err != nil {
			return err
		}
		if err := config.Init(*configPath); err != nil {
			return err
		}
		fmt.Println("wrote", *configPath)
		return nil
	case "key":
		return keyCommand(fs, configPath, args)
	case "scrub":
		data := fs.String("data", "", "data directory override")
		if err := fs.Parse(args); err != nil {
			return err
		}
		cfg, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		if *data != "" {
			cfg.DataDir = *data
		}
		return scrub(cfg.DataDir)
	case "health":
		if err := fs.Parse(args); err != nil {
			return err
		}
		url := "http://127.0.0.1:9000/_objex/health"
		if fs.NArg() > 0 {
			url = fs.Arg(0)
		}
		return health(url)
	case "help", "-h", "--help":
		fmt.Println(usage)
		return nil
	}
	return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
}

func serve(cfg config.Config, configPath string) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(logger)
	if len(cfg.Keys) == 0 {
		logger.Warn("no access keys configured: only anonymous reads of public buckets will work; create one with `objex key add <name>`")
	}
	srv, err := server.New(cfg, configPath, logger)
	if err != nil {
		return err
	}
	defer srv.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("objex listening", "address", cfg.Listen, "data", cfg.DataDir, "fsync", cfg.Fsync)
	return srv.Run(ctx)
}

func logLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(envOr("OBJEX_LOG", "info"))); err != nil {
		return slog.LevelInfo
	}
	return l
}

func keyCommand(fs *flag.FlagSet, configPath *string, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: objex key add|list|rm")
	}
	sub, args := args[0], args[1:]
	var buckets multi
	readOnly := false
	if sub == "add" {
		fs.Var(&buckets, "bucket", "restrict the key to this bucket (repeatable)")
		fs.BoolVar(&readOnly, "read-only", false, "only allow reads (GET, HEAD, list)")
	}
	// Allow flags after the positional name: objex key add app -read-only
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() == 0 {
			break
		}
		pos, args = append(pos, fs.Arg(0)), fs.Args()[1:]
	}
	switch sub {
	case "add":
		if len(pos) != 1 {
			return errors.New("usage: objex key add <name> [-bucket b]... [-read-only]")
		}
		k, err := config.AddKey(*configPath, pos[0], buckets, readOnly)
		if err != nil {
			return err
		}
		fmt.Printf("added key %q to %s\naccess key: %s\nsecret key: %s\n", k.Name, *configPath, k.AccessKey, k.SecretKey)
		return nil
	case "list":
		cfg, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		if len(cfg.Keys) == 0 {
			fmt.Println("no keys")
		}
		for _, k := range cfg.Keys {
			scope := "all buckets"
			if len(k.Buckets) > 0 {
				scope = strings.Join(k.Buckets, ",")
			}
			if k.ReadOnly {
				scope += " (read-only)"
			}
			fmt.Printf("%-24s %-22s %s\n", k.Name, k.AccessKey, scope)
		}
		return nil
	case "rm":
		if len(pos) != 1 {
			return errors.New("usage: objex key rm <access-key-or-name>")
		}
		n, err := config.RemoveKey(*configPath, pos[0])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("no key named or with access key %s", pos[0])
		}
		fmt.Printf("removed %d key(s)\n", n)
		return nil
	}
	return fmt.Errorf("unknown key command %q", sub)
}

func scrub(dir string) error {
	s, err := local.Open(dir, local.Options{VerifyReads: true})
	if err != nil {
		return err
	}
	defer s.Close()
	r, err := s.Scrub(0)
	if err != nil {
		return err
	}
	fmt.Printf("checked %d blob(s), %d bytes\n", r.Blobs, r.Bytes)
	for _, p := range r.Problems {
		fmt.Println("DAMAGED", p)
	}
	if len(r.Problems) > 0 {
		return fmt.Errorf("%d damaged blob(s)", len(r.Problems))
	}
	return nil
}

func health(url string) error {
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: %s %s", resp.Status, body)
	}
	fmt.Print(string(body))
	return nil
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

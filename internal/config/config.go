// Package config loads objex settings from a JSON file with OBJEX_*
// environment overrides, and manages access keys in that file.
package config

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Key struct {
	Name      string `json:"name"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	// Buckets restricts the key to these buckets; empty means all.
	Buckets []string `json:"buckets,omitempty"`
	// ReadOnly keys may only GET, HEAD and list.
	ReadOnly bool `json:"read_only,omitempty"`
}

// CanAccess reports whether the key may use bucket. A nil key (anonymous) may not.
func (k *Key) CanAccess(bucket string) bool {
	if k == nil {
		return false
	}
	if len(k.Buckets) == 0 {
		return true
	}
	for _, b := range k.Buckets {
		if b == "*" || b == bucket {
			return true
		}
	}
	return false
}

type Config struct {
	Listen  string `json:"listen"`
	DataDir string `json:"data_dir"`
	// Region is reported to clients; requests signed for any region are accepted.
	Region string `json:"region"`
	// Domain enables virtual-host addressing (bucket.domain).
	Domain string `json:"domain,omitempty"`
	// Fsync makes every acknowledged write durable.
	Fsync bool `json:"fsync"`
	// VerifyReads checks the stored checksum of objects read in full.
	VerifyReads bool `json:"verify_reads"`
	// MaxConnections caps concurrently served connections.
	MaxConnections int `json:"max_connections"`
	// ScrubIntervalHours between background integrity scrubs; 0 disables.
	ScrubIntervalHours int `json:"scrub_interval_hours"`
	// ScrubMBPerSec limits scrub read bandwidth; 0 is unlimited.
	ScrubMBPerSec int `json:"scrub_mb_per_sec"`
	// GCIntervalHours between sweeps for orphaned blob files; 0 disables.
	GCIntervalHours int   `json:"gc_interval_hours"`
	Keys            []Key `json:"keys"`
}

func Default() Config {
	return Config{
		Listen: "0.0.0.0:9000", DataDir: "./data", Region: "us-east-1", Fsync: true, VerifyReads: true,
		MaxConnections: 4096, ScrubIntervalHours: 168, ScrubMBPerSec: 64, GCIntervalHours: 24,
	}
}

// Load reads path (a missing file means defaults), then applies environment
// overrides. Unknown fields are rejected, so typos do not pass silently.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			d := json.NewDecoder(bytes.NewReader(data))
			d.DisallowUnknownFields()
			if err := d.Decode(&cfg); err != nil {
				return Config{}, fmt.Errorf("%s: %w", path, err)
			}
		case !errors.Is(err, os.ErrNotExist):
			return Config{}, err
		}
	}
	if err := applyEnv(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, cfg.validate()
}

func (c *Config) validate() error {
	if c.Listen == "" || c.DataDir == "" || c.Region == "" {
		return errors.New("listen, data_dir and region must not be empty")
	}
	if c.MaxConnections < 1 {
		return errors.New("max_connections must be at least 1")
	}
	if c.ScrubIntervalHours < 0 || c.ScrubMBPerSec < 0 || c.GCIntervalHours < 0 {
		return errors.New("scrub and gc settings must not be negative")
	}
	seen := map[string]bool{}
	for _, k := range c.Keys {
		if k.AccessKey == "" || k.SecretKey == "" {
			return fmt.Errorf("key %q needs access_key and secret_key", k.Name)
		}
		if seen[k.AccessKey] {
			return fmt.Errorf("duplicate access key %s", k.AccessKey)
		}
		seen[k.AccessKey] = true
	}
	return nil
}

func applyEnv(c *Config) error {
	str := func(name string, dst *string) {
		if v := os.Getenv(name); v != "" {
			*dst = v
		}
	}
	num := func(name string, dst *int) error {
		if v := os.Getenv(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			*dst = n
		}
		return nil
	}
	flag := func(name string, dst *bool) {
		if v := os.Getenv(name); v != "" {
			switch strings.ToLower(v) {
			case "0", "false", "no", "off":
				*dst = false
			default:
				*dst = true
			}
		}
	}
	str("OBJEX_LISTEN", &c.Listen)
	str("OBJEX_DATA_DIR", &c.DataDir)
	str("OBJEX_REGION", &c.Region)
	str("OBJEX_DOMAIN", &c.Domain)
	flag("OBJEX_FSYNC", &c.Fsync)
	flag("OBJEX_VERIFY_READS", &c.VerifyReads)
	for name, dst := range map[string]*int{
		"OBJEX_MAX_CONNECTIONS": &c.MaxConnections, "OBJEX_SCRUB_INTERVAL_HOURS": &c.ScrubIntervalHours,
		"OBJEX_SCRUB_MB_PER_SEC": &c.ScrubMBPerSec, "OBJEX_GC_INTERVAL_HOURS": &c.GCIntervalHours,
	} {
		if err := num(name, dst); err != nil {
			return err
		}
	}
	if ak, sk := os.Getenv("OBJEX_ACCESS_KEY"), os.Getenv("OBJEX_SECRET_KEY"); ak != "" && sk != "" {
		keys := c.Keys[:0:0]
		for _, k := range c.Keys {
			if k.AccessKey != ak {
				keys = append(keys, k)
			}
		}
		c.Keys = append(keys, Key{Name: "env", AccessKey: ak, SecretKey: sk})
	}
	c.Domain = strings.ToLower(strings.Trim(c.Domain, "."))
	c.DataDir = filepath.Clean(c.DataDir)
	return nil
}

// ---------------------------------------------------------------------------
// Managing the config file. It holds secret keys, so it is always written
// owner-only, atomically, and synced.
// ---------------------------------------------------------------------------

// fileConfig is what is stored on disk: the file's own contents, without
// environment overrides.
func readFile(path string) (map[string]json.RawMessage, error) {
	raw := map[string]json.RawMessage{}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raw, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return raw, nil
}

func fileKeys(raw map[string]json.RawMessage) ([]Key, error) {
	var keys []Key
	if v, ok := raw["keys"]; ok {
		if err := json.Unmarshal(v, &keys); err != nil {
			return nil, fmt.Errorf("keys: %w", err)
		}
	}
	return keys, nil
}

// writePrivate replaces path atomically with an owner-only file.
func writePrivate(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	// An existing temporary file keeps its old mode; tighten it.
	err = f.Chmod(0o600)
	if err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// Init writes a default config file. It fails if path exists.
func Init(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s already exists", path)
	}
	data, _ := json.MarshalIndent(Default(), "", "  ")
	return writePrivate(path, append(data, '\n'))
}

const (
	keyAlphabet    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	secretAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)

func randomString(n int, alphabet string) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	// 256 is not a multiple of the alphabet sizes; reject the biased tail.
	out := make([]byte, 0, n)
	limit := byte(256 - 256%len(alphabet))
	for len(out) < n {
		for _, c := range b {
			if c < limit && len(out) < n {
				out = append(out, alphabet[int(c)%len(alphabet)])
			}
		}
		if _, err := rand.Read(b); err != nil {
			panic(err)
		}
	}
	return string(out)
}

func setKeys(path string, raw map[string]json.RawMessage, keys []Key) error {
	if keys == nil {
		keys = []Key{}
	}
	v, _ := json.Marshal(keys)
	raw["keys"] = v
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	return writePrivate(path, append(data, '\n'))
}

// AddKey generates a key pair and stores it in the config file.
func AddKey(path, name string, buckets []string, readOnly bool) (Key, error) {
	raw, err := readFile(path)
	if err != nil {
		return Key{}, err
	}
	keys, err := fileKeys(raw)
	if err != nil {
		return Key{}, err
	}
	k := Key{Name: name, AccessKey: "OBX" + randomString(17, keyAlphabet), SecretKey: randomString(40, secretAlphabet), Buckets: buckets, ReadOnly: readOnly}
	return k, setKeys(path, raw, append(keys, k))
}

// RemoveKey removes keys matching an access key or name, returning how many.
func RemoveKey(path, keyOrName string) (int, error) {
	raw, err := readFile(path)
	if err != nil {
		return 0, err
	}
	keys, err := fileKeys(raw)
	if err != nil {
		return 0, err
	}
	kept := keys[:0:0]
	for _, k := range keys {
		if k.AccessKey != keyOrName && k.Name != keyOrName {
			kept = append(kept, k)
		}
	}
	removed := len(keys) - len(kept)
	if removed == 0 {
		return 0, nil
	}
	return removed, setKeys(path, raw, kept)
}

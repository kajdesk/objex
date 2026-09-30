package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKeyFileIsPrivateAndRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "objex.json")
	mode := func() os.FileMode { st, _ := os.Stat(path); return st.Mode().Perm() }
	if err := Init(path); err != nil {
		t.Fatal(err)
	}
	if err := Init(path); err == nil {
		t.Fatal("Init overwrote an existing file")
	}
	if mode() != 0o600 {
		t.Fatalf("init mode %v", mode())
	}
	k, err := AddKey(path, "app", []string{"uploads"}, true)
	if err != nil || !strings.HasPrefix(k.AccessKey, "OBX") || len(k.AccessKey) != 20 || len(k.SecretKey) != 40 {
		t.Fatalf("%+v %v", k, err)
	}
	if mode() != 0o600 {
		t.Fatalf("mode after add %v", mode())
	}
	cfg, err := Load(path)
	if err != nil || len(cfg.Keys) != 1 || !cfg.Keys[0].ReadOnly || !cfg.Keys[0].CanAccess("uploads") || cfg.Keys[0].CanAccess("other") {
		t.Fatalf("%+v %v", cfg, err)
	}
	if n, err := RemoveKey(path, "app"); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	// A pre-existing world-readable file is tightened on the next key change.
	_ = os.Chmod(path, 0o644)
	if _, err := AddKey(path, "b", nil, false); err != nil {
		t.Fatal(err)
	}
	if mode() != 0o600 {
		t.Fatalf("loose file not tightened: %v", mode())
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temporary file left behind")
	}
}

func TestLoadRejectsTyposAndApplyEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "objex.json")
	_ = os.WriteFile(path, []byte(`{"listen": "127.0.0.1:1", "fsnyc": false}`), 0o600)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "fsnyc") {
		t.Fatalf("typo accepted: %v", err)
	}
	t.Setenv("OBJEX_ACCESS_KEY", "OBXENV")
	t.Setenv("OBJEX_SECRET_KEY", "secret")
	t.Setenv("OBJEX_FSYNC", "off")
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil || cfg.Fsync || len(cfg.Keys) != 1 || cfg.Keys[0].AccessKey != "OBXENV" {
		t.Fatalf("%+v %v", cfg, err)
	}
	var anon *Key
	if anon.CanAccess("x") {
		t.Fatal("anonymous key can access")
	}
}

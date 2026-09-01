package clickhouse

import (
	"strings"
	"testing"
	"time"
)

func TestParseDSN(t *testing.T) {
	cfg, maxOpen, err := parseDSN("clickhouse://rio:secret@ch.example:19000/analytics?dial_timeout=3s&max_open_conns=4")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "ch.example:19000" || cfg.User != "rio" || cfg.Password != "secret" || cfg.Database != "analytics" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.Timeout != 3*time.Second || maxOpen != 4 || cfg.TLS != nil {
		t.Fatalf("timeout=%v maxOpen=%d tls=%v", cfg.Timeout, maxOpen, cfg.TLS)
	}
}

func TestParseDSNDefaults(t *testing.T) {
	cfg, maxOpen, err := parseDSN("clickhouse://localhost")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "localhost:9000" || cfg.User != "default" || cfg.Database != "default" || maxOpen != 0 {
		t.Fatalf("cfg = %+v maxOpen=%d", cfg, maxOpen)
	}
}

func TestParseDSNSecure(t *testing.T) {
	cfg, _, err := parseDSN("clickhouse://h:9440?secure=true&skip_verify=1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLS == nil || cfg.TLS.ServerName != "h" || !cfg.TLS.InsecureSkipVerify {
		t.Fatalf("tls = %+v", cfg.TLS)
	}
}

func TestParseDSNRejects(t *testing.T) {
	for dsn, want := range map[string]string{
		"http://localhost":                   "scheme",
		"clickhouse://h?compress=lz4":        "unsupported DSN parameter",
		"clickhouse://h?secure=maybe":        "bad DSN parameter",
		"clickhouse://h?dial_timeout=fast":   "bad DSN parameter",
		"clickhouse://h?max_open_conns=many": "bad DSN parameter",
	} {
		if _, _, err := parseDSN(dsn); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err = %v, want %q", dsn, err, want)
		}
	}
}

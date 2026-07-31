package gcm

import "testing"

func TestBuildConfigMatchesMainParameters(t *testing.T) {
	cfg, err := buildConfig(
		"127.0.0.1:1080",
		"gcm.ics.de5.net",
		3,
		"saas.sin.fan",
		"v2up",
		"128.199.255.242",
		"cloudflare-ech.com",
		"https://doh.pub/dns-query",
		true,
		false,
		true,
		true,
	)
	if err != nil {
		t.Fatalf("buildConfig() error = %v", err)
	}

	if cfg.ListenAddress != "127.0.0.1:1080" {
		t.Fatalf("ListenAddress = %q", cfg.ListenAddress)
	}
	if cfg.WorkerHost != "gcm.ics.de5.net" {
		t.Fatalf("WorkerHost = %q", cfg.WorkerHost)
	}
	if len(cfg.RelayIPs) != 1 || cfg.RelayIPs[0] != "saas.sin.fan" {
		t.Fatalf("RelayIPs = %#v", cfg.RelayIPs)
	}
	if cfg.UserID != "v2up" || cfg.ProxyIP != "128.199.255.242" {
		t.Fatalf("UserID = %q, ProxyIP = %q", cfg.UserID, cfg.ProxyIP)
	}
	if !cfg.EnableECH || cfg.ECHDomain != "cloudflare-ech.com" {
		t.Fatalf("EnableECH = %v, ECHDomain = %q", cfg.EnableECH, cfg.ECHDomain)
	}
	if !cfg.EnableDoH || cfg.DoHUrl != "https://doh.pub/dns-query" {
		t.Fatalf("EnableDoH = %v, DoHUrl = %q", cfg.EnableDoH, cfg.DoHUrl)
	}
	if !cfg.EnableDNSWarmup {
		t.Fatal("EnableDNSWarmup = false")
	}
	if cfg.MinPoolSize != 3 || cfg.MaxPoolSize != 3 {
		t.Fatalf("pool size = %d..%d", cfg.MinPoolSize, cfg.MaxPoolSize)
	}
}

func TestBuildConfigPreservesMainDoHFallback(t *testing.T) {
	cfg, err := buildConfig("127.0.0.1:1080", "wss://worker.example/", 0, "", "", "", "", "", false, false, false, false)
	if err != nil {
		t.Fatalf("buildConfig() error = %v", err)
	}
	if cfg.WorkerHost != "worker.example" {
		t.Fatalf("WorkerHost = %q", cfg.WorkerHost)
	}
	if cfg.DoHUrl != "" {
		t.Fatalf("DoHUrl = %q, want main fallback sentinel", cfg.DoHUrl)
	}
}

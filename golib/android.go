package gcm

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"gcm/config"
	"gcm/dns"
	"gcm/ech"
	"gcm/logger"
	"gcm/pool"
	"gcm/relay"
	"gcm/socks5"
)

var (
	lifecycleMu  sync.Mutex
	cfg          *config.Config
	relayManager *relay.RelayManager
	dnsCache     *dns.DNSCache
	dohClient    *dns.DoHClient
	echManager   *ech.EchManager
	connPool     *pool.ConnectionPool
	socks5Server *socks5.Server
)

// StartSocksProxy 启动 GCM 代理（gomobile AAR 入口）
func StartSocksProxy(listenAddr, workerHost string, wsConn int, relayIPs, userID, proxyIP, echDomain, dohURL string, enableECH, disableIPv6Route, verbose bool) error {
	_ = disableIPv6Route
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	if socks5Server != nil {
		return fmt.Errorf("GCM proxy is already running")
	}

	c := config.DefaultConfig()
	c.ListenAddress = listenAddr
	c.WorkerHost = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(workerHost, "wss://"), "https://"), "/")
	if wsConn > 0 {
		c.MinPoolSize, c.MaxPoolSize = wsConn, wsConn
	} else {
		c.MinPoolSize, c.MaxPoolSize = 3, 3
	}
	if relayIPs != "" {
		for _, item := range strings.Split(relayIPs, ",") {
			if item = strings.TrimSpace(item); item != "" {
				c.RelayIPs = append(c.RelayIPs, item)
			}
		}
	}
	c.UserID = userID
	c.ProxyIP = proxyIP
	if echDomain != "" {
		c.ECHDomain = echDomain
	}
	if dohURL != "" {
		c.DoHUrl = dohURL
	}
	c.EnableECH = enableECH
	if verbose {
		c.LogLevel = config.DEBUG
	} else {
		c.LogLevel = config.INFO
	}
	c.EnableLogFile = false
	c.EnableDNSWarmup = false
	c.EnableQualityMonitor = true

	logger.InitGlobalLogger(c)
	systemLog := logger.GetLogger("System")
	log.SetFlags(log.LstdFlags)

	d := dns.NewDoHClient(c)
	dc := dns.NewDNSCache(c, d)
	rm := relay.NewRelayManager(c.RelayIPs, c, dc)
	if err := rm.Init(); err != nil {
		dc.Close()
		logger.Close()
		return fmt.Errorf("initialize relay manager: %w", err)
	}

	var em *ech.EchManager
	if c.EnableECH {
		em = ech.NewEchManager(d, c.ECHDomain, c.GetECHCacheTTL(), c.GetECHRefreshInterval())
		if echConfig, err := d.GetECHConfig(c.ECHDomain); err == nil {
			em.CacheConfig(c.ECHDomain, echConfig)
		} else {
			systemLog.Warn("ECH 配置预取失败: %v (将回退到标准 TLS)", err)
		}
	}
	p := pool.NewConnectionPool(c, rm, em)
	if c.EnableDoHProxy {
		d.EnableProxy(pool.NewProxyTransport(p))
	}
	go func() {
		if err := p.Warmup(); err != nil {
			systemLog.Warn("连接池预热失败: %v", err)
		}
	}()
	s := socks5.NewServer(c, p, dc)
	if err := s.Start(); err != nil {
		p.Close()
		rm.Close()
		dc.Close()
		logger.Close()
		return fmt.Errorf("start SOCKS5 server: %w", err)
	}
	if c.EnableECH {
		em.StartAutoRefresh()
	}
	cfg, dohClient, dnsCache, relayManager, echManager, connPool, socks5Server = c, d, dc, rm, em, p, s
	systemLog.Info("GCM 已就绪")
	return nil
}

// StopSocksProxy 停止代理并逆序释放所有资源。
func StopSocksProxy() {
	go func() {
		lifecycleMu.Lock()
		defer lifecycleMu.Unlock()
		if socks5Server == nil {
			return
		}
		if echManager != nil {
			echManager.StopAutoRefresh()
		}
		_ = socks5Server.Close()
		connPool.Close()
		relayManager.Close()
		dnsCache.Close()
		logger.Close()
		cfg, dohClient, dnsCache, relayManager, echManager, connPool, socks5Server = nil, nil, nil, nil, nil, nil, nil
	}()
}

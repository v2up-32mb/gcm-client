// Package gcm 提供 GCM (Go Cloud Multiplexer) 的 Android 客户端核心库。
//
// 通过 gomobile 编译为 AAR，供 Android UI 层调用。
// 集成了 WebSocket 连接池、2 字节头二进制流复用协议、SOCKS5/HTTP 代理服务器、
// 乐观 SOCKS5 响应、窗口流量控制和 DNS 解析。
//
// 协议格式（2 字节头）: [STREAM_ID:1][TYPE:1][可选 DATA]
// TYPE = 0/1/2/3 对应 CONNECT / CONNECTED / DATA / CLOSE
// CONNECT 的 DATA 为 ASCII 文本 "host:port|"
package gcm

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ======================== 全局参数 ========================

var (
	workerHost    string // WebSocket 服务器域名（如 gcm.ics.de5.net）
	userID        string // 路径标识（如 v2up），WS URL = wss://workerHost/userID
	connectionNum int    // 每个中转 IP 的连接数
	relayAddr     string // 优选中转节点地址（host:port 或 IP:port，逗号分隔多个，空表示直连 Worker）
	fallbackIP    string // 出口代理 IP（逗号分隔多个，通过 WS URL 查询参数 ?fallbackip= 透传给 Worker）

	gcmPool        *GCMPool
	proxyListener  net.Listener
	enableVerbose  bool // 详细日志开关
)

// StartSocksProxy 启动 SOCKS5/HTTP 代理。Android 通过 gomobile AAR 调用此入口。
//
// 参数：
//   - listenAddr: 本地监听地址（如 "127.0.0.1:1080"）
//   - wsServer:   Worker 服务器域名（如 "gcm.ics.de5.net"），无需 wss:// 前缀
//   - n:          每个中转 IP 的 WebSocket 连接数（<=0 时默认 1）
//   - relay:      优选中转节点地址（host:port 或 IP:port，逗号分隔多个；空字符串表示直连 Worker）
//   - uid:        路径标识（如 "v2up"），用作 WS URL 路径
//   - fip:        出口代理 IP（逗号分隔多个，通过 WS URL 查询参数 ?fallbackip= 发送给 Worker）
//   - verbose:    是否输出详细日志
func StartSocksProxy(listenAddr, wsServer string, n int, relay, uid, fip string, verbose bool) error {
	if wsServer == "" {
		return fmt.Errorf("缺少 Worker 服务地址")
	}
	// 去除可能的前缀
	wsServer = strings.TrimPrefix(wsServer, "wss://")
	wsServer = strings.TrimPrefix(wsServer, "https://")
	wsServer = strings.TrimSuffix(wsServer, "/")

	workerHost = wsServer
	userID = uid
	if userID == "" {
		return fmt.Errorf("缺少用户 ID（路径标识）")
	}
	connectionNum = n
	if connectionNum <= 0 {
		connectionNum = 1
	}
	relayAddr = strings.TrimSpace(relay)
	fallbackIP = strings.TrimSpace(fip)
	enableVerbose = verbose

	if enableVerbose {
		log.Printf("[GCM] 启动代理: 监听=%s Worker=%s UserID=%s 连接数=%d 优选中转=%s 出口IP=%s",
			listenAddr, workerHost, userID, connectionNum, relayAddr, fallbackIP)
	}

	// 初始化连接池
	gcmPool = NewGCMPool(workerHost, userID, connectionNum, relayAddr, fallbackIP)
	gcmPool.Start()

	// 启动本地代理服务器
	go runProxyServer(listenAddr)
	return nil
}

// StopSocksProxy 停止代理并关闭连接池。
func StopSocksProxy() {
	go func() {
		if proxyListener != nil {
			_ = proxyListener.Close()
			proxyListener = nil
		}
		if gcmPool != nil {
			gcmPool.Close()
			gcmPool = nil
		}
	}()
}

func isNormalCloseError(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "use of closed") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "normal closure")
}

func logf(format string, a ...interface{}) {
	if enableVerbose {
		log.Printf(format, a...)
	}
}

// splitClean 用逗号分割字符串并去除空白项，用于解析可能含有端口的多个中转/出口 IP。
func splitClean(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}


// ======================== GCM 二进制协议 ========================
//
// 协议格式（2 字节头）: [STREAM_ID:1][TYPE:1][可选 DATA]
// TYPE = 0(CONNECT) / 1(CONNECTED) / 2(DATA) / 3(CLOSE)
// CONNECT 的 DATA 为 ASCII "host:port|"
// CONNECTED / CLOSE 无 DATA
// DATA 的 DATA 为任意二进制

const (
	msgConnect   = 0
	msgConnected = 1
	msgData      = 2
	msgClose     = 3
	headerSize   = 2
	maxStreams   = 256 // 1 字节 StreamID，最多 256 个流
)

func encodeConnectMsg(streamID byte, host string, port uint16) []byte {
	payload := fmt.Sprintf("%s:%d|", host, port)
	buf := make([]byte, headerSize+len(payload))
	buf[0] = streamID
	buf[1] = msgConnect
	copy(buf[2:], payload)
	return buf
}

func encodeDataMsg(streamID byte, data []byte) []byte {
	buf := make([]byte, headerSize+len(data))
	buf[0] = streamID
	buf[1] = msgData
	copy(buf[2:], data)
	return buf
}

func encodeCloseMsg(streamID byte) []byte {
	return []byte{streamID, msgClose}
}

// ======================== WebSocket 连接池 ========================

// GCMPool 管理 WebSocket 多路复用连接池。
type GCMPool struct {
	workerHost string
	userID     string
	connNum    int
	relayAddr  string // 优选中转节点地址（逗号分隔多个，TCP 中继时使用；空表示直连 Worker）
	fallbackIP string // 出口代理 IP（通过 WS URL 查询参数 ?fallbackip= 透传给 Worker）

	wsConns   []*websocket.Conn
	wsMutexes []sync.Mutex
	connCount int32

	// 流管理：StreamID -> 所属的 WS 连接索引
	streamMapMu sync.RWMutex
	streamMap    map[byte]int  // streamID -> wsConn index
	streamConns  map[byte]net.Conn // streamID -> client TCP conn
	streamClosed map[byte]bool // streamID -> closed flag

	// 客户端到 Worker 的数据转发
	dataChanMu sync.Mutex
	dataChans   map[byte]chan []byte // streamID -> 传输到 Worker 的数据 channel

	closing bool
}

// NewGCMPool 创建连接池。
func NewGCMPool(host, uid string, n int, relay, fip string) *GCMPool {
	return &GCMPool{
		workerHost:  host,
		userID:      uid,
		connNum:     n,
		relayAddr:   relay,
		fallbackIP:  fip,
		wsConns:     make([]*websocket.Conn, n),
		wsMutexes:   make([]sync.Mutex, n),
		streamMap:   make(map[byte]int),
		streamConns: make(map[byte]net.Conn),
		streamClosed: make(map[byte]bool),
		dataChans:   make(map[byte]chan []byte),
	}
}

// Start 启动所有 WebSocket 连接。
func (p *GCMPool) Start() {
	for i := 0; i < p.connNum; i++ {
		go p.dialOnce(i)
	}
}

// Close 关闭连接池。
func (p *GCMPool) Close() {
	p.streamMapMu.Lock()
	p.closing = true
	for _, c := range p.streamConns {
		_ = c.Close()
	}
	p.streamMapMu.Unlock()

	for i, ws := range p.wsConns {
		if ws != nil {
			p.wsMutexes[i].Lock()
			_ = ws.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(300*time.Millisecond))
			_ = ws.Close()
			p.wsMutexes[i].Unlock()
		}
	}
}

// dialWebSocket 建立 WebSocket 连接。
func (p *GCMPool) dialWebSocket() (*websocket.Conn, error) {
	url := fmt.Sprintf("wss://%s/%s", p.workerHost, p.userID)
	// 出口代理 IP 透传给 Worker：作为查询参数 ?fallbackip= 发送。
	// 支持逗号分隔的多个 IP，过滤空白项后以英文逗号拼接传给 Worker。
	if p.fallbackIP != "" {
		parts := strings.Split(p.fallbackIP, ",")
		cleaned := make([]string, 0, len(parts))
		for _, ip := range parts {
			if v := strings.TrimSpace(ip); v != "" {
				cleaned = append(cleaned, v)
			}
		}
		if len(cleaned) > 0 {
			url += "?fallbackip=" + neturl.QueryEscape(strings.Join(cleaned, ","))
		}
	}
	serverName := p.workerHost

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: serverName,
	}

	headers := make(http.Header)
	headers.Set("Host", p.workerHost)
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36")

	dialer := websocket.Dialer{
		TLSClientConfig:  tlsConfig,
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   65536,
		WriteBufferSize:  65536,
	}

	// 中转模式：将 TCP 连接到优选的中转节点而非直接连 Worker
	if p.relayAddr != "" {
		relays := splitClean(p.relayAddr)
		dialer.NetDial = func(network, addr string) (net.Conn, error) {
			target := relays[int(time.Now().UnixNano())%len(relays)]
			logf("[GCM] 优选中转: %s -> %s (TLS SNI: %s)", addr, target, p.workerHost)
			return net.DialTimeout(network, target, 10*time.Second)
		}
	} else {
		dialer.NetDial = func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 10*time.Second)
		}
	}

	wsConn, _, err := dialer.Dial(url, headers)
	if err != nil {
		return nil, err
	}

	// 设置 WC 超时和初始 buffer size
	wsConn.SetReadLimit(0) // 不限制读取
	return wsConn, nil
}

func (p *GCMPool) dialOnce(index int) {
	for {
		if p.closing {
			return
		}
		wsConn, err := p.dialWebSocket()
		if err != nil {
			logf("[GCM] 连接池[%d]连接失败: %v，2秒后重试", index, err)
			time.Sleep(2 * time.Second)
			continue
		}
		p.wsConns[index] = wsConn
		logf("[GCM] 连接池[%d]已连接 -> wss://%s/%s", index, p.workerHost, p.userID)
		go p.handleMessages(index, wsConn)
		return
	}
}

func (p *GCMPool) redial(index int) {
	logf("[GCM] 连接池[%d]重连中...", index)
	p.wsConns[index] = nil
	// 清理该连接上所有 stream
	p.streamMapMu.Lock()
	for sid, idx := range p.streamMap {
		if idx == index {
			if c, ok := p.streamConns[sid]; ok {
				_ = c.Close()
			}
			delete(p.streamMap, sid)
			delete(p.streamConns, sid)
			delete(p.streamClosed, sid)
			p.closeDataChannel(sid)
		}
	}
	p.streamMapMu.Unlock()
	go p.dialOnce(index)
}

// handleMessages 读取 WebSocket 消息并路由到对应的 stream。
func (p *GCMPool) handleMessages(index int, wsConn *websocket.Conn) {
	// Ping/Pong 心跳
	wsConn.SetPingHandler(func(msg string) error {
		p.wsMutexes[index].Lock()
		err := wsConn.WriteMessage(websocket.PongMessage, []byte(msg))
		p.wsMutexes[index].Unlock()
		return err
	})

	// 启动心跳 goroutine
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if p.closing {
					return
				}
				p.wsMutexes[index].Lock()
				_ = wsConn.WriteMessage(websocket.PingMessage, nil)
				p.wsMutexes[index].Unlock()
			}
		}
	}()

	for {
		if p.closing {
			return
		}
		_, msg, err := wsConn.ReadMessage()
		if err != nil {
			if !isNormalCloseError(err) {
				logf("[GCM] 连接池[%d]读取失败: %v", index, err)
			}
			if !p.closing {
				p.redial(index)
			}
			return
		}

		if len(msg) < headerSize {
			continue
		}

		streamID := msg[0]
		msgType := msg[1]
		data := msg[2:]

		p.streamMapMu.RLock()
		clientConn, hasConn := p.streamConns[streamID]
		isClosed := p.streamClosed[streamID]
		p.streamMapMu.RUnlock()

		switch msgType {
		case msgConnected:
			// CONNECTED 到达 — 流已建立成功，无需特殊处理（乐观响应已在 agent 侧提前发送）
			logf("[GCM] Stream[%02x] CONNECTED (池[%d])", streamID, index)
			// 通知数据转发 goroutine 可以开始传输到 Worker
			p.signalDataReady(streamID)

		case msgData:
			if !hasConn || isClosed {
				continue
			}
			if len(data) > 0 {
				if _, werr := clientConn.Write(data); werr != nil {
					logf("[GCM] Stream[%02x] 写客户端失败: %v", streamID, werr)
					p.cleanupStream(streamID)
				}
			}

		case msgClose:
			logf("[GCM] Stream[%02x] 收到 CLOSE (池[%d])", streamID, index)
			p.cleanupStream(streamID)
		}
	}
}

// allocateStream 分配一个新的 StreamID 并绑定到指定 WS 连接。
// 返回 streamID 或 -1 表示无可用 stream。
func (p *GCMPool) allocateStream(index int, target string, clientConn net.Conn) int {
	p.streamMapMu.Lock()
	defer p.streamMapMu.Unlock()

	// 查找空闲 streamID
	for i := 0; i < 256; i++ {
		sid := byte(i)
		if _, exists := p.streamMap[sid]; exists {
			continue
		}
		// 分配
		p.streamMap[sid] = index
		p.streamConns[sid] = clientConn
		p.streamClosed[sid] = false

		// 发送 CONNECT 消息
		port := uint16(443) // 默认端口
		if strings.Contains(target, ":") {
			host, portStr, err := net.SplitHostPort(target)
			if err == nil {
				var p2 uint16
				fmt.Sscanf(portStr, "%d", &p2)
				if p2 > 0 {
					port = p2
				}
				target = host
			}
		}
		if port == 0 {
			port = 443
		}

		// 如果是 IPv6，用方括号
		connectPayload := encodeConnectMsg(sid, target, port)
		p.wsMutexes[index].Lock()
		err := p.wsConns[index].WriteMessage(websocket.BinaryMessage, connectPayload)
		p.wsMutexes[index].Unlock()
		if err != nil {
			delete(p.streamMap, sid)
			delete(p.streamConns, sid)
			delete(p.streamClosed, sid)
			return -1
		}

		// 创建数据转发 channel
		p.dataChans[sid] = make(chan []byte, 64)
		return int(sid)
	}
	// 所有 streamID 都已用完
	return -1
}

// sendToWorker 通过 dataChan 异步将数据发送到 Worker。
func (p *GCMPool) sendToWorker(streamID byte, data []byte) error {
	p.streamMapMu.RLock()
	index, ok := p.streamMap[streamID]
	ws := p.wsConns[index]
	p.streamMapMu.RUnlock()
	if !ok || ws == nil {
		return fmt.Errorf("stream not allocated")
	}

	dataMsg := encodeDataMsg(streamID, data)
	p.wsMutexes[index].Lock()
	err := ws.WriteMessage(websocket.BinaryMessage, dataMsg)
	p.wsMutexes[index].Unlock()
	return err
}

// sendClose 发送 CLOSE 给 Worker 并清理。
func (p *GCMPool) sendClose(streamID byte) {
	p.streamMapMu.Lock()
	index, ok := p.streamMap[streamID]
	p.streamMapMu.Unlock()
	if !ok {
		return
	}
	closeMsg := encodeCloseMsg(streamID)
	if p.wsConns[index] != nil {
		p.wsMutexes[index].Lock()
		_ = p.wsConns[index].WriteMessage(websocket.BinaryMessage, closeMsg)
		p.wsMutexes[index].Unlock()
	}
}

// signalDataReady 通知数据转发 goroutine 可以开始传输（CONNECTED 到达）。
func (p *GCMPool) signalDataReady(streamID byte) {
	// no-op in current design，data forward 即时转发；
	// 保留接口以备未来同步流控时使用
	_ = streamID
}

// closeDataChannel 关闭并删除 stream 的数据传输 channel。
func (p *GCMPool) closeDataChannel(streamID byte) {
	p.dataChanMu.Lock()
	if ch, ok := p.dataChans[streamID]; ok {
		close(ch)
		delete(p.dataChans, streamID)
	}
	p.dataChanMu.Unlock()
}

// cleanupStream 清理单个 stream 的资源。
func (p *GCMPool) cleanupStream(streamID byte) {
	p.streamMapMu.Lock()
	if c, ok := p.streamConns[streamID]; ok {
		_ = c.Close()
		delete(p.streamConns, streamID)
	}
	delete(p.streamMap, streamID)
	delete(p.streamClosed, streamID)
	p.streamMapMu.Unlock()
	p.closeDataChannel(streamID)
}

// pickConnection 选一个可用的 WebSocket 连接（轮询）。
func (p *GCMPool) pickConnection() int {
	for i := 0; i < len(p.wsConns); i++ {
		if p.wsConns[i] != nil {
			return i
		}
	}
	return -1
}

// ======================== 代理服务器 ========================

type proxyConfig struct {
	Username string
	Password string
	Host     string
}

func runProxyServer(addr string) {
	config, err := parseProxyAddr(addr)
	if err != nil {
		log.Fatalf("[GCM] 解析代理地址失败: %v", err)
	}

	listener, err := net.Listen("tcp", config.Host)
	if err != nil {
		log.Fatalf("[GCM] 代理监听失败 %s: %v", config.Host, err)
	}
	proxyListener = listener
	log.Printf("[GCM] 代理服务器启动: %s", config.Host)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if isNormalCloseError(err) {
				return
			}
			continue
		}
		go handleProxyConnection(conn, config)
	}
}

func parseProxyAddr(addr string) (*proxyConfig, error) {
	config := &proxyConfig{}
	if strings.Contains(addr, "@") {
		parts := strings.SplitN(addr, "@", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("无效的代理地址格式")
		}
		if strings.Contains(parts[0], ":") {
			authParts := strings.SplitN(parts[0], ":", 2)
			config.Username = authParts[0]
			config.Password = authParts[1]
		}
		config.Host = parts[1]
	} else {
		config.Host = addr
	}
	return config, nil
}

func handleProxyConnection(conn net.Conn, config *proxyConfig) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return
	}

	switch buf[0] {
	case 0x05:
		handleSOCKS5(conn, config)
	case 'G', 'P', 'C', 'H', 'D', 'O':
		handleHTTP(conn, config, buf[0])
	}
}

// ======================== SOCKS5 ========================

const (
	noAuth       = 0x00
	userPassAuth = 0x02
	noAcceptable = 0xFF

	ipv4Addr   = 0x01
	domainAddr = 0x03
	ipv6Addr   = 0x04

	succeeded           = 0x00
	generalFailure      = 0x01
	commandNotSupported = 0x07
)

func handleSOCKS5(conn net.Conn, config *proxyConfig) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return
	}
	methods := make([]byte, buf[0])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}

	var method uint8 = noAuth
	if config.Username != "" {
		method = userPassAuth
		found := false
		for _, m := range methods {
			if m == userPassAuth {
				found = true
				break
			}
		}
		if !found {
			method = noAcceptable
		}
	}

	conn.Write([]byte{0x05, method})
	if method == noAcceptable {
		return
	}

	if method == userPassAuth {
		if !handleSOCKS5Auth(conn, config) {
			return
		}
	}

	handleSOCKS5Request(conn, config)
}

func handleSOCKS5Auth(conn net.Conn, config *proxyConfig) bool {
	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil || buf[0] != 1 {
		return false
	}
	user := make([]byte, buf[1])
	if _, err := io.ReadFull(conn, user); err != nil {
		return false
	}
	buf = make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return false
	}
	pass := make([]byte, buf[0])
	if _, err := io.ReadFull(conn, pass); err != nil {
		return false
	}

	status := byte(0x00)
	if string(user) != config.Username || string(pass) != config.Password {
		status = 0x01
	}
	conn.Write([]byte{0x01, status})
	return status == 0x00
}

func handleSOCKS5Request(conn net.Conn, config *proxyConfig) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || buf[0] != 5 {
		return
	}
	cmd, atyp := buf[1], buf[3]

	var host string
	switch atyp {
	case ipv4Addr:
		b := make([]byte, 4)
		io.ReadFull(conn, b)
		host = net.IP(b).String()
	case domainAddr:
		b := make([]byte, 1)
		io.ReadFull(conn, b)
		d := make([]byte, b[0])
		io.ReadFull(conn, d)
		host = string(d)
	case ipv6Addr:
		b := make([]byte, 16)
		io.ReadFull(conn, b)
		host = net.IP(b).String()
	default:
		conn.Write([]byte{0x05, 0x08, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}

	portBuf := make([]byte, 2)
	io.ReadFull(conn, portBuf)
	port := int(portBuf[0])<<8 | int(portBuf[1])

	target := fmt.Sprintf("%s:%d", host, port)
	if atyp == ipv6Addr {
		target = fmt.Sprintf("[%s]:%d", host, port)
	}

	switch cmd {
	case 0x01: // CONNECT
		handleSOCKS5Connect(conn, target)
	case 0x03: // UDP ASSOCIATE 暂不做
		conn.Write([]byte{0x05, commandNotSupported, 0, 1, 0, 0, 0, 0, 0, 0})
	default:
		conn.Write([]byte{0x05, commandNotSupported, 0, 1, 0, 0, 0, 0, 0, 0})
	}
}

// handleSOCKS5Connect 处理 SOCKS5 CONNECT 请求。
// 使用乐观响应：发 CONNECT 后立即回 SOCKS5 成功，不等 CONNECTED 返回。
func handleSOCKS5Connect(conn net.Conn, target string) {
	if gcmPool == nil {
		conn.Write([]byte{0x05, generalFailure, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}

	// 选取一个 WS 连接
	index := gcmPool.pickConnection()
	if index < 0 {
		// 暂时等待连接池就绪 2 秒
		time.Sleep(2 * time.Second)
		index = gcmPool.pickConnection()
		if index < 0 {
			conn.Write([]byte{0x05, generalFailure, 0, 1, 0, 0, 0, 0, 0, 0})
			return
		}
	}

	// 分配 Stream 并发送 CONNECT
	sid := gcmPool.allocateStream(index, target, conn)
	if sid < 0 {
		conn.Write([]byte{0x05, generalFailure, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	streamID := byte(sid)
	conn.SetDeadline(time.Time{}) // 清除之前的 deadline

	logf("[GCM] 新请求 -> %s | 池[%d] Stream[%02x]", target, index, streamID)

	// 乐观响应：立即回 SOCKS5 成功，不等 CONNECTED
	conn.Write([]byte{0x05, succeeded, 0x00, 0x01, 0, 0, 0, 0, 0, 0})

	// 启动客户端数据转发 goroutine
	go func() {
		defer func() {
			gcmPool.sendClose(streamID)
			gcmPool.cleanupStream(streamID)
		}()

		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			if werr := gcmPool.sendToWorker(streamID, buf[:n]); werr != nil {
				logf("[GCM] Stream[%02x] 发送失败: %v", streamID, werr)
				return
			}
		}
	}()

	// 在主 goroutine 中等待客户端关闭或被清理（通过检测连接是否被 pool 关闭）
	// 客户端 -> Worker 数据由上面的 goroutine 处理
	// Worker -> 客户端数据由 handleMessages 处理
	// 这里阻塞等待 stream 被清理
	// 实际上：上面的 goroutine 退出时会清理 stream，conn 也会被关闭

	// 等待 stream 结束（被 handleMessages 清理或转发 goroutine 退出时清理）
	// 转发 goroutine 退出时已调用 cleanupStream 和 sendClose，
	// 然后该 goroutine 也会关闭 conn。主调用方只需等 stream 从映射中消失。
	for {
		gcmPool.streamMapMu.RLock()
		_, has := gcmPool.streamMap[streamID]
		closed := gcmPool.streamClosed[streamID]
		gcmPool.streamMapMu.RUnlock()
		if !has || closed {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ======================== HTTP 代理 ========================

func handleHTTP(conn net.Conn, _ *proxyConfig, _ byte) {
	// 简化实现：对非 SOCKS5 流量暂不做 HTTP 代理（heur-socks5-tunnel 处理 HTTP/HTTPS 转发到 SOCKS5）
	// 也可以做 CONNECT 隧道，但 hev-socks5-tunnel 会把所有流量都走 SOCKS5
	conn.Write([]byte("HTTP/1.1 501 Not Implemented\r\n\r\n"))
}

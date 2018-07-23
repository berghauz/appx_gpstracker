package main

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

// connection type
type connection struct {
	ws      *websocket.Conn
	ctx     *Context
	appxURI string
	appxID  string
	alive   bool
	msgRx   int64
}

// connPool type
type connPool struct {
	connections map[*connection]bool
	mu          sync.Mutex
}

// WsConnect func
func (ctx *Context) WsConnect(uri string) (*websocket.Conn, error) {

	u, err := url.Parse(uri)
	if err != nil {
		logger.WithFields(log.Fields{"uri": uri}).Fatalf("Error parse ws uri %+v", err)
	}

	dialer := websocket.Dialer{
		ReadBufferSize:    1024,
		WriteBufferSize:   1024,
		EnableCompression: false,
	}

	if u.Scheme == "wss" {

		cer, err := tls.LoadX509KeyPair(ctx.SSL.Certificate, ctx.SSL.PublicKey)
		if err != nil {
			logger.WithFields(log.Fields{"uri": uri, "crt": ctx.SSL.Certificate, "key": ctx.SSL.PublicKey}).Fatalf("Something goes wrong with SSL certs loading %+v", err)
		}
		dialer.TLSClientConfig = &tls.Config{Certificates: []tls.Certificate{cer}}
	}

	wsHeaders := http.Header{
		"Origin":                   {u.Host},
		"Sec-WebSocket-Extensions": {"permessage-deflate; client_max_window_bits;"},
	}

	//logger.Infof("Trying connect to %s", uri)
	conn, _, err := dialer.Dial(uri, wsHeaders)
	if err == nil {
		//logger.WithFields(log.Fields{"uri": uri, "crt": ctx.SSL.Certificate, "key": ctx.SSL.PublicKey}).Fatalf("%+v", err)
		logger.Infof("Connected to %s", uri)
	}

	return conn, err
}

// GetAppxs func
func (ctx *Context) GetAppxs() {

	type invite struct {
		Owner string `json:"owner"`
	}

	conn, err := ctx.WsConnect(ctx.Owner.AppxBootstrapURI)
	if err != nil {
		logger.WithFields(log.Fields{"uri": ctx.Owner.AppxBootstrapURI, "crt": ctx.SSL.Certificate, "key": ctx.SSL.PublicKey}).Fatalf("GetAppxs WsConnect %+v", err)
		//logger.Infof("Connected to %s", uri)
	}
	defer conn.Close()

	err = conn.WriteJSON(map[string]interface{}{"owner": ctx.Owner.ID})
	if err != nil {
		logger.WithFields(log.Fields{"uri": ctx.Owner.AppxBootstrapURI, "owner": ctx.Owner.ID}).Fatalf("Fail send appxs request %+v", err)
	}

	err = conn.ReadJSON(&ctx.Appxs)
	if err != nil {
		logger.WithFields(log.Fields{"uri": ctx.Owner.AppxBootstrapURI, "owner": ctx.Owner.ID}).Fatalf("Fail read appxs response %+v", err)
	}
	if len(ctx.Appxs.AppxList) == 0 {
		logger.WithFields(log.Fields{"uri": ctx.Owner.AppxBootstrapURI, "owner": ctx.Owner.ID}).Fatalf("AppxList empty %+v", ctx.Appxs.AppxList)
	}
}

// ListenAppxNode func
func (conn *connection) ListenAppxNode(appxMessage chan<- AppxMessage) {
	var err error
	for {
		var value []byte
		var appxMsg = AppxMessage{conn.appxURI, conn.appxID, value}
		if conn.alive {
			_, appxMsg.Message, err = conn.ws.ReadMessage()
			if err != nil {
				logger.Warnf("ListenAppxNode %s %v.", conn.appxURI, err)
				conn.alive = false
				break
			}
			appxMessage <- appxMsg
			conn.msgRx++
			rawMessagesRecieved.WithLabelValues(conn.ctx.AppName, conn.appxID, conn.appxURI).Inc()
		}
		// select {
		// case <-shutdown:
		// 	conn.ws.Close()
		// 	logger.WithFields(log.Fields{"appx_id": conn.appxID, "appx_uri": conn.appxURI}).Info("Listener closed")
		// 	wggs.Done()
		// 	return
		// default:
		// 	break
		// }
	}
}

func (conn *connection) Respawn(timeout time.Duration, appxMessage chan<- AppxMessage) {
	var err error
	logger.Warnf("Trying to reconnect to %s in %v", conn.appxURI, timeout)
	ticker := time.NewTicker(timeout)
	//defer ticker.Stop()

	for {

		if conn.alive {
			break
		}

		select {
		case <-ticker.C:
			conn.ws, err = conn.ctx.WsConnect(conn.appxURI)
			if err != nil {
				logger.Warnf("Cant't respawn connection %s %+v, trying again in %v", conn.appxURI, err, timeout)
				//return
			} else {
				go conn.ListenAppxNode(appxMessage)
				go conn.keepAlive(time.Duration(*keepAlive)*time.Second, appxMessage)
				conn.alive = true
				ticker.Stop()
			}
		}
	}
}

func (conn *connection) keepAlive(timeout time.Duration, appxMessage chan<- AppxMessage) {
	lastResponse := time.Now()
	conn.ws.SetPongHandler(func(msg string) error {
		lastResponse = time.Now()
		return nil
	})

	go func() {
		for {
			err := conn.ws.WriteMessage(websocket.PingMessage, []byte("keepalive"))
			if err != nil {
				logger.Fatalf("Failed to send ping %+v", err)
			}
			time.Sleep(timeout / 2)
			if time.Now().Sub(lastResponse) > timeout {
				conn.alive = false
				conn.ws.Close()
				conn.Respawn(time.Duration(*respawnTimeout)*time.Second, appxMessage)
				break
			}
		}
	}()
}

func (p *connPool) Add(conn *connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	//p.connections = append(p.connections, conn)
	p.connections[conn] = true
}

func (p *connPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for conn := range p.connections {
		logger.Infof("Appxid %s endpoint %s served %v messages", conn.appxID, conn.appxURI, conn.msgRx)
		conn.ws.WriteMessage(websocket.CloseMessage, []byte{})
		if err := conn.ws.Close(); err != nil {
			logger.Warningf("Error closing %s %+v", conn.appxURI, err)
		} else {
			logger.Infof("Closing %s", conn.appxURI)
		}
	}
}

// newConnPool func
func newConnPool() *connPool {
	return &connPool{
		connections: make(map[*connection]bool),
	}
}

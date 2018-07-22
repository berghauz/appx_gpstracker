package main

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var appxMessage = make(chan AppxMessage)
var pool = new(connPool)

func main() {

	ctx := CreateContext(*confFile)

	interrupt := make(chan os.Signal)
	sighup := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	signal.Notify(sighup, os.Interrupt, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-interrupt:
				logger.Println("EXIT")
				pool.CloseAll()
				os.Exit(0)
			case <-sighup:
				logger.Println("Reloading filters...")
				ctx.ReloadFilters(*confFile)
				logger.Println("Reloading filters done. New is %+v", ctx.Filters)
			}
		}
	}()

	ctx.GetAppxs()
	logger.Printf("%+v", ctx)

	for _, uri := range ctx.Appxs.AppxList {
		if *backLog {
			uri.URI = uri.URI + "/?upid=0"
		}
		c, err := ctx.WsConnect(uri.URI)
		if err != nil {
			logger.WithFields(log.Fields{"uri": uri.URI, "crt": ctx.SSL.Certificate, "key": ctx.SSL.PublicKey}).Fatalf("Bootstrap connection loop %+v", err)
		}

		conn := connection{c, ctx, uri.URI, uri.Appxid, true, 0}
		pool.Add(&conn)

		go conn.ListenAppxNode()
		go conn.keepAlive(time.Duration(*keepAlive) * time.Second)
		logger.Printf("Listen on %s, appxid %v in boostrap loop", uri.URI, uri.Appxid)
	}

	go ctx.HandleNewMessage(appxMessage)

	// conn := ctx.WsConnect(ctx.Appxs.AppxList[0].URI)
	// go ServeConnection(conn)

	http.Handle("/metrics", promhttp.Handler())
	panic(http.ListenAndServe(":9002", nil))
	//select {}
}

package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var pool = newConnPool()

var (
	version    = "UNDEFINED"
	buildstamp = "UNDEFINED"
	githash    = "UNDEFINED"
)

// gracefull shutdown helpers
var wggs = sync.WaitGroup{}
var shutdown = make(chan struct{})

func main() {

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx := CreateContext(*confFile)
	fmt.Printf("%s %s\nGIT Commit Hash: %s\nBuild Time: %s\n\n", ctx.AppName, version, githash, buildstamp)
	ctx.LoadDecoders()
	ctx.InitBackends()
	appxProxyInfo.WithLabelValues(ctx.AppName, ctx.Owner.ID).Set(1)

	appxMessage := make(chan AppxMessage, ctx.Owner.QueueFlushCount*3)

	interrupt := make(chan os.Signal)
	sighup := make(chan os.Signal)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(sighup, syscall.SIGHUP)

	// it is not strict necessary spliting the interrupt and sighup events hadnling, but just in case...
	go func() {
		<-interrupt
		logger.Infoln("Preparation of a graceful shutdown")
		pool.CloseAll()
		close(shutdown)
		wggs.Wait()
		// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//ctx.reSession.Close()
		pprof.StopCPUProfile()
		logger.Infoln("Horaaay...")
		os.Exit(0)
	}()

	go func() {
		for {
			select {
			case <-sighup:
				logger.Infoln("Reloading filters...")
				ctx.ReloadConfig(*confFile)
				logger.Infof("Reloading filters done. New is %+v", ctx.Filters)
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
			logger.WithFields(log.Fields{"uri": uri.URI, "crt": ctx.SSL.Certificate, "key": ctx.SSL.PrivateKey}).Fatalf("Bootstrap connection loop %+v", err)
		}

		wggs.Add(1)

		conn := connection{
			c, ctx, uri.URI,
			uri.Appxid, true, 0}
		pool.Add(&conn)

		go conn.ListenAppxNode(appxMessage)
		go conn.keepAlive(time.Duration(*keepAlive)*time.Second, appxMessage)
		wsConnections.Inc()
		logger.Infof("Listen on %s, appxid %v in boostrap loop", uri.URI, uri.Appxid)
	}

	go ctx.QueueProcessing(appxMessage, &wggs)

	http.Handle("/metrics", promhttp.Handler())
	panic(http.ListenAndServe(":"+*promPort, nil))
	//select {}
}

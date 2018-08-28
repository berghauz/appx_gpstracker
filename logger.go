package main

import (
	"flag"
	"os"

	"github.com/sirupsen/logrus"
)

var (
	confFile       *string
	logFile        *string
	respawnTimeout *int64
	keepAlive      *int64
	backLog        *bool
	logLevel       *string
	promPort       *string
	logger         *logrus.Logger
)

func init() {

	confFile = flag.String("C", "/etc/test/gpstrack.yaml", "config file full path")
	logFile = flag.String("L", "stdout", "log file path or stdout")
	keepAlive = flag.Int64("K", 5, "keepalive interval, sec")
	respawnTimeout = flag.Int64("R", 10, "respawn interval, sec")
	backLog = flag.Bool("b", false, "read backloged messages(bugged)")
	logLevel = flag.String("l", "info", "logging level (info, error, critical, debug...)")
	promPort = flag.String("p", "9002", "prometheus source port")
	flag.Parse()

	logger = logrus.New()

	formatter := &logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05-07:00",
		DisableColors:   false,
	}
	logger.Formatter = formatter

	ll, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		ll = logrus.InfoLevel
	}

	logger.SetLevel(ll)

	if *logFile != "stdout" {
		// prepare log file
		handle, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			logger.WithFields(logrus.Fields{"logFile": logFile}).Fatalf("%+v", err)
		}
		logger.Out = handle
	} else {
		logger.Out = os.Stdout
	}

	//logger.Println(*confFile, *logFile)
}

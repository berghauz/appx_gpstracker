package main

import (
	"io/ioutil"
	"regexp"

	"github.com/go-yaml/yaml"

	log "github.com/sirupsen/logrus"
)

// Context type
type Context struct {
	AppName string `yaml:"appname,omitempty"`
	Version int    `yaml:"version,omitempty"`
	Owner   struct {
		ID               string   `yaml:"id,omitempty"`
		AppxBootstrapURI string   `yaml:"appx_bootstrap_uri,omitempty"`
		StoragePrefList  []string `yaml:"storage_pref_list,omitempty"`
		QueueFlushCount  int      `yaml:"queue_flush_count,omitempty"`
		QueueFlushTime   int64    `yaml:"queue_flush_time,omitempty"`
	} `yaml:"owner,omitempty"`
	SSL struct {
		Certificate string `yaml:"certificate,omitempty"`
		PublicKey   string `yaml:"public_key,omitempty"`
		TrustChain  string `yaml:"trust_chain,omitempty"`
	} `yaml:"ssl,omitempty"`
	Mongo struct {
		URI string `yaml:"uri,omitempty"`
	} `yaml:"mongo,omitempty"`
	Filters struct {
		DevEui  []string `yaml:"deveui,omitempty"`
		MsgType []string `yaml:"msg_type,omitempty"`
	} `yaml:"filters,omitempty"`
	Appxs            TCIOInstance
	CompilledFilters *DevEuiFilters
}

// TCIOInstance type
type TCIOInstance struct {
	Error    string          `json:"error,omitempty"`
	Owner    string          `json:"owner,omitempty"`
	AppxList []ExchangePoint `json:"appx_list,omitempty"`
	Version  uint32          `json:"version,omitempty"`
	Release  uint32          `json:"release,omitempty"`
}

// ExchangePoint type
type ExchangePoint struct {
	Appxid string `json:"appxid,omitempty"`
	URI    string `json:"uri,omitempty"`
}

// DevEuiFilters type
type DevEuiFilters struct {
	ReExpressions []*regexp.Regexp
	//mu            sync.Mutex
}

//var devEuiFilters *DevEuiFilters

// CreateContext func
func CreateContext(config string) *Context {

	ctx := Context{}
	raw, err := ioutil.ReadFile(config)
	if err != nil {
		logger.WithFields(log.Fields{"config": config}).Fatalf("Can't load config file %+v", err)
	}

	err = yaml.Unmarshal(raw, &ctx)
	if err != nil {
		logger.WithFields(log.Fields{"config": config}).Fatalf("Can't parse config file %+v", err)
	}

	ctx.CompileFilters()
	return &ctx
}

// CompileFilters func
func (ctx *Context) CompileFilters() /**DevEuiFilters*/ {
	//ctx.CompilledFilters.mu.Lock()
	//defer ctx.CompilledFilters.mu.Unlock()
	df := DevEuiFilters{}

	for _, expr := range ctx.Filters.DevEui {
		df.ReExpressions = append(df.ReExpressions, regexp.MustCompile(expr))
	}
	ctx.CompilledFilters = &df
}

// ReloadFilters func
func (ctx *Context) ReloadFilters(config string) {

	tmp := Context{}

	raw, err := ioutil.ReadFile(config)
	if err != nil {
		logger.WithFields(log.Fields{"config": config}).Fatalf("Can't load config file %+v", err)
	}

	err = yaml.Unmarshal(raw, &tmp)
	if err != nil {
		logger.WithFields(log.Fields{"config": config}).Fatalf("Can't parse config file %+v", err)
	}

	ctx.Filters = tmp.Filters
	ctx.CompileFilters()
}

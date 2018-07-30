package main

import (
	"io/ioutil"
	"regexp"

	"github.com/go-yaml/yaml"
	log "github.com/sirupsen/logrus"
	re "gopkg.in/gorethink/gorethink.v4"
)

// Context type
type Context struct {
	AppName string `yaml:"appname"`
	Version int    `yaml:"version"`
	Owner   struct {
		ID               string   `yaml:"id"`
		AppxBootstrapURI string   `yaml:"appx_bootstrap_uri"`
		StoragePrefList  []string `yaml:"storage_pref_list"`
		QueueFlushCount  int      `yaml:"queue_flush_count"`
		QueueFlushTime   int64    `yaml:"queue_flush_time"`
	} `yaml:"owner"`
	SSL struct {
		Certificate string `yaml:"certificate"`
		PublicKey   string `yaml:"public_key"`
		TrustChain  string `yaml:"trust_chain"`
	} `yaml:"ssl"`
	Mongo struct {
		URI string `yaml:"uri"`
	} `yaml:"mongo"`
	RethinkDB struct {
		URI        string   `yaml:"uri"`
		URIs       []string `yaml:"uris"`
		DB         string   `yaml:"db"`
		InitialCap int      `yaml:"initial_cap"`
		MaxOpen    int      `yaml:"max_open"`
	} `yaml:"rethinkdb"`
	Filters struct {
		DevEui  []string `yaml:"deveui"`
		MsgType []string `yaml:"msg_type"`
	} `yaml:"filters"`
	Inventory        map[string]string `yaml:"inventory"`
	Appxs            TCIOInstance
	CompilledFilters *DevEuiFilters
	reSession        *re.Session
}

// TCIOInstance type
type TCIOInstance struct {
	Error    string          `json:"error"`
	Owner    string          `json:"owner"`
	AppxList []ExchangePoint `json:"appx_list"`
	Version  uint32          `json:"version"`
	Release  uint32          `json:"release"`
}

// ExchangePoint type
type ExchangePoint struct {
	Appxid string `json:"appxid"`
	URI    string `json:"uri"`
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

	ctx.reSession, err = re.Connect(re.ConnectOpts{
		Address:    ctx.RethinkDB.URI,
		Addresses:  ctx.RethinkDB.URIs,
		InitialCap: ctx.RethinkDB.InitialCap,
		MaxOpen:    ctx.RethinkDB.MaxOpen,
	})
	if err != nil {
		logger.Fatalf("Error connecting to rethinkdb %+v", err)
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

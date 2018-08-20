package main

import (
	"io/ioutil"
	"path/filepath"
	"plugin"
	"regexp"

	"github.com/go-yaml/yaml"
	log "github.com/sirupsen/logrus"
	re "gopkg.in/gorethink/gorethink.v4"
	es "gopkg.in/olivere/elastic.v5"
)

// Context type
type Context struct {
	AppName  string `yaml:"appname"`
	Version  int    `yaml:"version"`
	Decoders struct {
		Path string `yaml:"path"`
	} `yaml:"decoders"`
	Owner struct {
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
		Collection string   `yaml:"collection"`
		InitialCap int      `yaml:"initial_cap"`
		MaxOpen    int      `yaml:"max_open"`
	} `yaml:"rethinkdb"`
	Elastic struct {
		Hosts []string `yaml:"hosts"`
		Index string   `yaml:"index"`
	} `yaml:"elastic"`
	Filters struct {
		DevEui  []string `yaml:"deveui"`
		MsgType []string `yaml:"msg_type"`
	} `yaml:"filters"`
	Inventory        map[string]string `yaml:"inventory"`
	DecodePlugins    map[string]func(string) (interface{}, error)
	Appxs            TCIOInstance
	CompilledFilters *DevEuiFilters
	reSession        *re.Session
	esClient         *es.Client
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

type Decoder struct {
	Type    string
	Version string
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

	ctx.esClient, err = es.NewClient(es.SetURL(ctx.Elastic.Hosts...))
	if err != nil {
		logger.Fatalf("Error connecting to elastic %+v", err)
	}
	//logger.Infof("ES %+v", ctx.esClient.String())

	ctx.CompileFilters()
	ctx.LoadDecoders()
	// p, err := plugin.Open("/home/berg/go/src/github.com/berghauz/appx_decoders/TN-T0004001/TN-T0004001.so")
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("%+v\n", p)

	// t, err := p.Lookup("Test")
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(reflect.TypeOf(t))

	// info, err := p.Lookup("Decoder")
	// if err != nil {
	// 	panic(err)
	// }
	// var dec Decoder
	// fmt.Println("loaded: ", reflect.TypeOf(info))
	// fmt.Println("local: ", reflect.TypeOf(dec))
	// pg := info.(Decoder)
	// fmt.Println(*info.(*string))

	// decode, err := p.Lookup("Decode")
	// if err != nil {
	// 	panic(err)
	// }
	//a := decode.(func(string) (interface{}, error))
	// fmt.Println(reflect.TypeOf(decode))
	// ctx.DecodePlugins[*info.(*string)] = decode.(func(string) (interface{}, error))

	// all_plugins, err := filepath.Glob("plugins/*.so")
	// if err != nil {
	//     panic(err)
	// }

	return &ctx
}

// LoadDecoders func
func (ctx *Context) LoadDecoders() {
	// init decoders map
	ctx.DecodePlugins = make(map[string]func(string) (interface{}, error))
	allDecoders, err := filepath.Glob(ctx.Decoders.Path + "/*.so")
	if err != nil {
		logger.WithFields(log.Fields{"path": ctx.Decoders.Path}).Fatalf("Can't list decoders dir: %v", err)
	}

	for _, decoder := range allDecoders {
		// try to load decoder
		p, err := plugin.Open(decoder)
		if err != nil {
			logger.WithFields(log.Fields{"decoder": decoder}).Fatalf("Can't load decoder: %v", err)
		}
		// import descriptive type of decoder
		decoderType, err := p.Lookup("Decoder")
		if err != nil {
			logger.WithFields(log.Fields{"decoder": decoder}).Fatalf("Can't import type of decoder: %v", err)
		}
		// import decoder method
		decodeMethod, err := p.Lookup("Decode")
		if err != nil {
			logger.WithFields(log.Fields{"decoder": decoder}).Fatalf("Can't import decoder method: %v", err)
		}
		ctx.DecodePlugins[*decoderType.(*string)] = decodeMethod.(func(string) (interface{}, error))
	}

	for decoderType := range ctx.DecodePlugins {
		logger.Infof("Decoder loaded: %s", decoderType)
	}
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

// CheckRethinkAlive func
func (ctx *Context) CheckRethinkAlive() {
	var err error
	if !ctx.reSession.IsConnected() {
		ctx.reSession, err = re.Connect(re.ConnectOpts{
			Address:    ctx.RethinkDB.URI,
			Addresses:  ctx.RethinkDB.URIs,
			InitialCap: ctx.RethinkDB.InitialCap,
			MaxOpen:    ctx.RethinkDB.MaxOpen,
		})
		if err != nil {
			logger.Fatalf("Error REconnecting to rethinkdb %+v", err)
		}
		logger.Warnln("Lost rethinkdb connect, reconnected")
	}
}

// CheckElasticAlive func
func (ctx *Context) CheckElasticAlive() {

}

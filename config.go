package main

import (
	"crypto/tls"
	"io/ioutil"
	"path/filepath"
	"plugin"
	"regexp"
	"time"

	"github.com/eclipse/paho.mqtt.golang"

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
		PrivateKey  string `yaml:"private_key"`
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
	Mqtt struct {
		Brokers     []string `yaml:"brokers"`
		Certificate string   `yaml:"certificate"`
		PrivateKey  string   `yaml:"private_key"`
		User        string   `yaml:"user"`
		Password    string   `yaml:"password"`
		DnTopic     string   `yaml:"dntopic"`
		UpTopic     string   `yaml:"uptopic"`
		UpQoS       byte     `yaml:"upqos"`
		DnQoS       byte     `yaml:"dnqos"`
	} `yaml:"mqtt"`
	Filters struct {
		DevEui  []string `yaml:"deveui"`
		MsgType []string `yaml:"msg_type"`
	} `yaml:"filters"`
	Inventory        map[string]string `yaml:"inventory"`
	DecodingPlugins  map[string]func(string) (interface{}, error)
	Appxs            TCIOInstance
	CompilledFilters *DevEuiFilters
	reSession        *re.Session
	esClient         *es.Client
	mqttClient       mqtt.Client
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

	ctx.CompileFilters()
	ctx.InitBackends()

	return &ctx
}

// InitBackends func
func (ctx *Context) InitBackends() {
	for _, storage := range ctx.Owner.StoragePrefList {
		var err error
		switch storage {
		case "rethinkdb":
			if ctx.RethinkDB.URI != "" && ctx.RethinkDB.DB != "" && ctx.RethinkDB.Collection != "" {
				ctx.reSession, err = re.Connect(re.ConnectOpts{
					Address:    ctx.RethinkDB.URI,
					Addresses:  ctx.RethinkDB.URIs,
					InitialCap: ctx.RethinkDB.InitialCap,
					MaxOpen:    ctx.RethinkDB.MaxOpen,
				})
				if err != nil {
					logger.Fatalf("Error connecting to %s %+v", storage, err)
				}
				break
			}
			logger.Fatalf("%s listed in pipeline but not configured: %+v", storage, ctx.RethinkDB)
			break
		case "elastic":
			if len(ctx.Elastic.Hosts) != 0 && ctx.Elastic.Index != "" {
				ctx.esClient, err = es.NewClient(es.SetURL(ctx.Elastic.Hosts...))
				if err != nil {
					logger.Fatalf("Error connecting to %s %+v", storage, err)
				}
				break
			}
			logger.Fatalf("%s listed in pipeline but not configured: %+v", storage, ctx.Elastic)
			break
		case "mqtt":
			if len(ctx.Mqtt.Brokers) != 0 && ctx.Mqtt.User != "" && ctx.Mqtt.Password != "" && ctx.Mqtt.DnTopic != "" && ctx.Mqtt.UpTopic != "" {
				ctx.createMqttConnection()
				// for _, broker := range ctx.Mqtt.Brokers {
				// 	opts = mqtt.NewClientOptions().AddBroker(broker)
				// }

				// cer, err := tls.LoadX509KeyPair(ctx.Mqtt.Certificate, ctx.Mqtt.PrivateKey)
				// if err != nil {
				// 	logger.Fatalf("Something goes wrong with MQTT SSL certs loading %+v", err)
				// }

				// opts.SetUsername(ctx.Mqtt.User)
				// opts.SetPassword(ctx.Mqtt.Password)
				// opts.SetClientID(ctx.AppName)
				// opts.SetConnectTimeout(time.Second * 3)
				// opts.SetTLSConfig(&tls.Config{Certificates: []tls.Certificate{cer}, InsecureSkipVerify: true})
				// opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
				// 	logger.Errorln("Mqtt disconnected, trying to reconnect...")
				// 	ctx.createMqttConnection()
				// })
				// ctx.mqttClient = mqtt.NewClient(opts)

				// if token := ctx.mqttClient.Connect(); token.Wait() && token.Error() != nil {
				// 	logger.Fatalf("Error connecting to %s %+v", storage, token.Error())
				// }

				// if token := ctx.mqttClient.Subscribe(ctx.Mqtt.DnTopic, ctx.Mqtt.UpQoS, pool.handleMqttDnMessage); token.Wait() && token.Error() != nil {
				// 	logger.Fatalf("Error subscribe to mqtt uptopic %s: %+v", ctx.Mqtt.UpTopic, token.Error())
				// }
				break
			}
			logger.Fatalf("%s listed in pipeline but not configured: %+v", storage, ctx.Mqtt)
			break
		}
	}
}

// should make a unifie function rather than use code duplication
func (ctx *Context) createMqttConnection() {
	ticker := time.NewTicker(time.Second * 1)
	// if ctx.mqttClient.IsConnected() {
	// 	ctx.mqttClient.Disconnect(1000)
	// }
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var opts *mqtt.ClientOptions
			for _, broker := range ctx.Mqtt.Brokers {
				opts = mqtt.NewClientOptions().AddBroker(broker)
			}

			/// ADD SSL AUTODETECT
			cer, err := tls.LoadX509KeyPair(ctx.Mqtt.Certificate, ctx.Mqtt.PrivateKey)
			if err != nil {
				logger.Fatalf("Something goes wrong with MQTT SSL certs loading %+v", err)
			}

			opts.SetUsername(ctx.Mqtt.User)
			opts.SetPassword(ctx.Mqtt.Password)
			opts.SetClientID(ctx.AppName)
			opts.SetConnectTimeout(time.Second * 3)
			opts.SetTLSConfig(&tls.Config{Certificates: []tls.Certificate{cer}, InsecureSkipVerify: true})
			opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
				logger.Errorln("Mqtt disconnected, trying to reconnect...")
				ctx.createMqttConnection()
			})
			ctx.mqttClient = mqtt.NewClient(opts)

			if token := ctx.mqttClient.Connect(); token.Wait() && token.Error() != nil {
				logger.Errorf("Error connecting to mqtt %+v", token.Error())
				ticker = time.NewTicker(time.Second * 1)
			} else {
				logger.Infoln("Mqtt reconnected")
				if token := ctx.mqttClient.Subscribe(ctx.Mqtt.DnTopic, ctx.Mqtt.UpQoS, pool.handleMqttDnMessage); token.Wait() && token.Error() != nil {
					logger.Errorf("Error subscribe to mqtt uptopic %s: %+v", ctx.Mqtt.UpTopic, token.Error())
				}
				ticker.Stop()
				return
			}
		}
	}
}

// LoadDecoders func
func (ctx *Context) LoadDecoders() {
	// init decoders map
	ctx.DecodingPlugins = make(map[string]func(string) (interface{}, error))
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
		ctx.DecodingPlugins[*decoderType.(*string)] = decodeMethod.(func(string) (interface{}, error))
	}

	for decoderType := range ctx.DecodingPlugins {
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

// ReloadConfig func
func (ctx *Context) ReloadConfig(config string) {

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
	ctx.Inventory = tmp.Inventory
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

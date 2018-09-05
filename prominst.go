package main

import "github.com/prometheus/client_golang/prometheus"

var appxProxyInfo = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "appx_proxy_info",
		Help: "Common appx proxy information",
	},
	[]string{"appx_name", "owner_id"},
)

var rawMessagesRecieved = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_raw_messages_recieved",
		Help: "Raw messages received by given TCIO instance",
	},
	[]string{"appx_id", "tcio_url"},
)

var messagesRecievedByFilter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_recieved_by_filter",
		Help: "Raw messages received by proxy has hitted filters",
	},
	[]string{"msg_type"},
)

var messagesPassedFilter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_passed_by_filter",
		Help: "Raw messages received by proxy has passed filters",
	},
	[]string{"msg_type"},
)

var messagesDroppedByType = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_dropped_by_type_wl",
		Help: "Messages dropped by type whitelist",
	},
	[]string{"msg_type"},
)

var messagesDroppedByDeveui = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_dropped_by_deveui_wl",
		Help: "Messages dropped by deveui whitelist",
	},
	[]string{"msg_type"},
)

var messagesStoredInRethinkDb = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_stored_rethinkdb",
		Help: "Messages stored in RethinkDB",
	},
)

var messagesStoredInElastic = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_stored_elastic",
		Help: "Messages stored in ElasticSearch",
	},
)

var messagesPublishedToMqtt = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_published_mqtt",
		Help: "Messages pushed to MQTT",
	},
)

var messagesReceivedFromMqtt = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_received_mqtt",
		Help: "Messages received from MQTT",
	},
)

var messagesForwardedToTcio = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_forwarded_to_tcio",
		Help: "Messages received from MQTT and forwarded to TCIO",
	},
	[]string{"appx_id", "tcio_url"},
)

var messagesDroppedFromMqtt = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_dropped_mqtt",
		Help: "Messages from MQTT dropped due error",
	},
	[]string{"error"},
)

var wsConnections = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "appx_ws_connections",
		Help: "Number of current LNS ws connections",
	},
)

var wsPingSent = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_ws_ping_sent",
		Help: "Number of pings sent to ws",
	},
)

var wsPongRcvd = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_ws_pong_rcvd",
		Help: "Number of pongs received from ws",
	},
)

var messagesHittedDecoder = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_hitted_decoder",
		Help: "Messages hitted decoder",
	},
)

var messagesDecoded = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_decoded_success",
		Help: "Messages successfuly decoded",
	},
)

var messagesDecodingFailed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_decoding_failed",
		Help: "Messages failed to decode",
	},
)

var messagesDecoderNotFound = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_decoder_not_found",
		Help: "Messages failed to decode due to no decoder found",
	},
)

var messagesLeavedWithoutDecoding = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_messages_leaved_without_decoding",
		Help: "Messages leaved without decoding",
	},
)

var mqttPublishHistogram = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "appx_mqtt_duration_millis",
		Help:    "MQTT publish duration histogram",
		Buckets: prometheus.ExponentialBuckets(1, 10, 5),
	},
)

var rethinkPublishHistogram = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "appx_rethink_duration_millis",
		Help:    "Rethink publish duration histogram",
		Buckets: prometheus.ExponentialBuckets(1, 10, 5),
	},
)

var elasticPublishHistogram = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "appx_elastic_duration_millis",
		Help:    "Elastic publish duration histogram",
		Buckets: prometheus.ExponentialBuckets(1, 10, 5),
	},
)

var mqttPublishFailed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_mqtt_messages_push_fail",
		Help: "Messages failed to pushing into MQTT",
	},
)

var elasticInsertFailed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_elastic_messages_insert_fail",
		Help: "Messages failed to insert into ElasticSearch",
	},
)

var rethinkInsertFailed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_rethink_messages_insert_fail",
		Help: "Messages failed to insert into RethinkDB",
	},
)

var queueTimeFlushTimes = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_queue_flushed_by_time",
		Help: "Times queue flushed by time threshhold",
	},
)
var queueSizeFlushTimes = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "appx_queue_flushed_by_size",
		Help: "Times queue flushed by size threshhold",
	},
)

func init() {
	prometheus.MustRegister(appxProxyInfo,
		rawMessagesRecieved,
		messagesRecievedByFilter,
		messagesDroppedByType,
		messagesDroppedByDeveui,
		messagesStoredInRethinkDb,
		messagesPassedFilter,
		wsConnections,
		wsPingSent,
		wsPongRcvd,
		messagesStoredInElastic,
		messagesPublishedToMqtt,
		messagesReceivedFromMqtt,
		messagesDroppedFromMqtt,
		messagesHittedDecoder,
		messagesDecoded,
		messagesDecodingFailed,
		mqttPublishHistogram,
		rethinkPublishHistogram,
		elasticPublishHistogram,
		mqttPublishFailed,
		elasticInsertFailed,
		rethinkInsertFailed,
		messagesForwardedToTcio,
		messagesDecoderNotFound,
		messagesLeavedWithoutDecoding,
		queueTimeFlushTimes,
		queueSizeFlushTimes,
	)
}

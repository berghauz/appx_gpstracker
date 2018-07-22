package main

import "github.com/prometheus/client_golang/prometheus"

var rawMessagesRecieved = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_raw_messages_recieved",
		Help: "Raw messages recieved from given CTIO instance",
	},
	[]string{"app", "appx_id", "appx_url"},
)

var messagesRecievedByFilter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_recieved_by_filter",
		Help: "Raw messages recieved from given CTIO instance has hitted filters",
	},
	[]string{"app", "appx_id", "appx_url", "msg_type"},
)

var messagesPassedFilter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_passed_by_filter",
		Help: "Raw messages recieved from given CTIO instance has passed filters",
	},
	[]string{"app", "appx_id", "appx_url", "msg_type"},
)

var messagesDroppedByType = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_dropped_by_type_wl",
		Help: "Messages dropped within CTIO by type whitelist",
	},
	[]string{"app", "appx_id", "appx_url", "msg_type"},
)

var messagesDroppedByDeveui = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_dropped_by_deveui_wl",
		Help: "Messages dropped within CTIO by deveui whitelist",
	},
	[]string{"app", "appx_id", "appx_url", "msg_type"},
)

var messagesStoredByType = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "appx_messages_stored_in_db",
		Help: "Messages stored in DB within CTIO by type",
	},
	[]string{"app", "appx_id", "appx_url", "msg_type"},
)

func init() {
	prometheus.MustRegister(rawMessagesRecieved, messagesRecievedByFilter, messagesDroppedByType, messagesDroppedByDeveui, messagesStoredByType, messagesPassedFilter)
}

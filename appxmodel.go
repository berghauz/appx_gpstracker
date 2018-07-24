package main

import (
	"encoding/json"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

/*
EUI64		string of the form HH-HH-HH-HH-HH-HH-HH-HH whereby HH represents two uppercase hex digits
HEX32		string of the form HH..HH with exactly 32 uppercase hexdigits
HEX*		string with sequence of pairs of hex digits; can be empty
BOOL		boolean value, true or false
INT4		decimal integer in the range -2^31 .. 2^31-1
INT8		decimal integer in the range -2^63 .. 2^63-1
UINT4		decimal integer in the range 0 .. 2^32-1
UINT8		decimal integer in the range 0 .. 2^64-1
TIMESPAN 	string representation of a timespan \d+(.\d+)?)(ms|m|s|h|d), e.g. 1d, 10m
ISODATE 	string of the form YYYY-MM-DD (UTC time)
MACADDR 	a string of 6 octets denoting a 48-bit MAC address such as 00:A3:C9:14:C8:F1
ID6 		string representaion of a 64-bit number in the same way as a IPv6 address (e.g. “1::”, “::0”, “abc::12a”)
URI 		string forming a URI
WSURI 		string forming a websocket URI (ws://.. or wss://..)
UPID 		INT8 and unique message number per service end point
*/

/*
{
	"msgtype": 		"dndf"
	"MsgId": 		INT8
	"FPort": 		UINT1	// port number on the device
	"FRMPayload": 	HEX*	// down data frame payload (may be empty)
	"DevEui": 		EUI64	// identity of the device
	"confirm": 		bool	// confirmed down data frame?
}
*/

// DNDFModel type
type DNDFModel struct {
	MsgType    string `json:"msgtype"`
	MsgID      int64  `json:"MsgId"`
	FPort      uint8  `json:"FPort"`
	FRMPayload string `json:"FRMPayload"`
	DevEui     string `json:"DevEui"`
	Confirm    bool   `json:"confirm"`
}

/*
{
	"msgtype": 		"updf"
	"DevEui":		EUI64 	// device identifier
	"upid": 		INT8 	// unique message identifier
	"SessID": 		INT4 	// session identifier (0=ABP, >0 OTAA join sessions)
	"FCntUp": 		UINT4 	// frame counter used by the device
	"FPort": 		UINT1 	// port number
	"FRMPayload": 	HEX* 	// up data frame payload (may be empty)
	"DR": 			int 	// data rate the message was sent with
	"Freq":			int 	// frequency the message was sent with
	"region"		str 	// region specifier
}
*/

// UPDFModel type
type UPDFModel struct {
	MsgType    string `json:"msgtype"`
	DevEui     string `json:"DevEui"`
	UPID       int64  `json:"upid"`
	SessID     int32  `json:"SessID"`
	FCntUp     uint32 `json:"FCntUp"`
	FPort      uint8  `json:"FPort"`
	FRMPayload string `json:"FRMPayload"`
	DR         int64  `json:"DR"`
	Freq       int64  `json:"Freq"`
	Region     string `json:"region"`
}

/*
{
	"msgtype": 		"upinfo"
	"DevEui": 		EUI64	// device identifier
	"upid": 		INT8	// unique message identifier
	"SessID": 		INT4	// session identifier
	"FCntUp": 		UINT4	// frame counter used by the device
	"FPort": 		UINT1	// port number
	"FRMPayload": 	HEX*	// up data frame payload (may be empty)
	"DR": 			int		// data rate the message was sent with
	"Freq": 		int		// frequency the message was sent with
	"region": 		str		// region specifier
	"upinfo": 		[ UOBJ, .. ]
}

// upinfo:[map[muxid:0 xtime:7.8018215e+08 RxDelay:0 routerid:2.81474976710656e+14 snr:10 rssi:100 ArrTime:1.5322045932285311e+09 RX1DRoff:0 regionid:1 doorid:0 rxtime:1.5322045932263746e+09]

UOBJ = {
	"routerid": 	INT8 // identifier of the router having received the frame
	"muxid": 		INT8 // internal routing information
	"rssi": 		float // signal strength
	"snr": 			float // signal to noise ratio
	"ArrTime": 		float // arrival time stamp in the de-mux fabric
	#### not documented
	"xtime":
	"RxDelay":
	"RX1DRoff":
	"doorid":
	"rxtime":
}
*/

// UpInfoModel type
type UpInfoModel struct {
	UPDFModel
	UpInfo struct {
		RouterID int64   `json:"routerid"`
		MuxID    int64   `json:"muxid"`
		RSSI     float64 `json:"rssi"`
		SNR      float64 `json:"snr"`
		ArrTime  float64 `json:"ArrTime"`
		Xtime    float64 `json:"xtime"`    // expected type
		RxDelay  int64   `json:"RxDelay"`  // expected type
		RX1DRoff int64   `json:"RX1DRoff"` // expected type
		DoorID   int64   `json:"doorid"`   // expected type
		RxTime   float64 `json:"rxtime"`   // expected type
	} `json:"upinfo"`
}

// fake is a fake model, used as filtering match helper
type fake struct {
	MsgType string `json:"msgtype"`
	DevEui  string `json:"DevEui"`
}

// AppxMessage type
type AppxMessage struct {
	AppxURL string
	AppxID  string
	Message []byte
}

// FilterMessage func
func (ctx *Context) FilterMessage(message AppxMessage) bool {

	f := fake{}
	json.Unmarshal(message.Message, &f)
	messagesRecievedByFilter.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL, f.MsgType).Inc()
	for _, allowedType := range ctx.Filters.MsgType {
		if f.MsgType == allowedType || allowedType == "*" {
			for _, allowedDeveui := range ctx.CompilledFilters.ReExpressions {
				if allowedDeveui.MatchString(f.DevEui) {
					messagesPassedFilter.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL, f.MsgType).Inc()
					logger.WithFields(log.Fields{"uri": message.AppxURL, "id": message.AppxID}).Debugf("%+v", message.Message)
					return true
				}
				messagesDroppedByDeveui.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL, f.MsgType).Inc()
			}
		} else {
			messagesDroppedByType.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL, f.MsgType).Inc()
		}
	}
	return false
}

// QueueProcessing func
func (ctx *Context) QueueProcessing(message <-chan AppxMessage, wg *sync.WaitGroup) {
	var buf []AppxMessage
	var timeout = time.Duration(ctx.Owner.QueueFlushTime) * time.Millisecond
	flushTime := time.NewTicker(timeout)
	for {
		select {
		case newMsg := <-message:
			buf = append(buf, newMsg)
			if len(buf) == ctx.Owner.QueueFlushCount {
				ctx.dummysinc(buf)
				buf = nil
				break
			}
		case <-flushTime.C:
			ctx.dummysinc(buf)
			buf = nil
			flushTime = time.NewTicker(timeout)
			break
		}
	}
}

// dummysinc

func (ctx *Context) dummysinc(batch []AppxMessage) {
	for idx, msg := range batch {
		log.Infof("%v, %v=>%v <%+v>", idx, msg.AppxID, msg.AppxURL, msg.Message)
	}
}

/*
func (ctx *Context) QueueProcessing(msg <-chan AppxMessage) {
	for {
		f := fake{}
		newMsg := <-msg
		json.Unmarshal(newMsg.Message, &f)

		messagesRecievedByFilter.WithLabelValues(ctx.AppName, newMsg.AppxID, newMsg.AppxURL, f.MsgType).Inc()

		for _, allowedType := range ctx.Filters.MsgType {
			if f.MsgType == allowedType || allowedType == "*" {
				for _, allowedDeveui := range ctx.CompilledFilters.ReExpressions {
					if allowedDeveui.MatchString(f.DevEui) {
						messagesPassedFilter.WithLabelValues(ctx.AppName, newMsg.AppxID, newMsg.AppxURL, f.MsgType).Inc()
						logger.WithFields(log.Fields{"uri": newMsg.AppxURL, "id": newMsg.AppxID}).Debugf("%+v", newMsg.Message)
					} else {
						messagesDroppedByDeveui.WithLabelValues(ctx.AppName, newMsg.AppxID, newMsg.AppxURL, f.MsgType).Inc()
					}
				}
			} else {
				messagesDroppedByType.WithLabelValues(ctx.AppName, newMsg.AppxID, newMsg.AppxURL, f.MsgType).Inc()
			}
		}
	}
}
*/

/*
	for _, allowedType := range ctx.Filters.MsgType {
		if f.MsgType == allowedType || allowedType == "*" {
			for _, allowedDeveui := range ctx.Filters.DevEui {
				if f.DevEui == allowedDeveui || allowedDeveui == "*" {
					messagesPassedFilter.WithLabelValues(ctx.AppName, newMsg.AppxID, newMsg.AppxURL, f.MsgType).Inc()
					logger.WithFields(log.Fields{"uri": newMsg.AppxURL, "id": newMsg.AppxID}).Debugf("%+v", newMsg.Message)
				} else {
					messagesDroppedByDeveui.WithLabelValues(ctx.AppName, newMsg.AppxID, newMsg.AppxURL, f.MsgType).Inc()
				}
			}
		} else {
			messagesDroppedByType.WithLabelValues(ctx.AppName, newMsg.AppxID, newMsg.AppxURL, f.MsgType).Inc()
		}
	}

*/

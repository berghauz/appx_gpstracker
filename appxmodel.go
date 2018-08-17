package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	re "gopkg.in/gorethink/gorethink.v4"
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

// DnDfTracknet type
// Submits a downstream message for transmission within the next downlink window for a given device.
type DnDfTracknet struct {
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

/*

"AFCntDown": 253 ,
"AppSKeyEnv": null ,
"ArrTime": 1532602005.487868 ,
+"DR": 5 ,
"DevAddr": 13782224 ,
+"DevEui":  "64-7F-DA-00-00-00-07-85" ,
+"FCntUp": 5174 ,
+"FPort": 10 ,
+"FRMPayload":  "018808814905BF1400629800FF0152" ,
+"Freq": 868300000 ,
+"SessID": 169696865413625 ,
"ciphered": false ,
"confirm": false ,
"dClass":  "A" ,
+"msgtype":  "updf" ,
+"region":  "EU863" ,
"regionid": 1 ,
+"upid": 49373205491740460

*/

// UpDfTracknet type
// Reports an upstream message with its up data frame payload as transmitted by a given device and additional context information. This message is
// forwarded without delay and, if a given up data frame is received by multiple gateways, only the first copy received is forwarded.
type UpDfTracknet struct {
	MsgType    string    `json:"msgtype"`
	DevEui     string    `json:"DevEui"`
	UPID       int64     `json:"upid"`
	SessID     int32     `json:"SessID"`
	FCntUp     uint32    `json:"FCntUp"`
	FPort      uint8     `json:"FPort"`
	FRMPayload string    `json:"FRMPayload"`
	DR         int64     `json:"DR"`
	Freq       int64     `json:"Freq"`
	Region     string    `json:"region"`
	ArrTime    time.Time `json:"ArrTime"`
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

// UpInfoTracknet type
// Reports additional context information about an up data frame as received from a given device. This information usually arrives a short time after a
// corresponding updf message because forwarding is delayed to take into account context information from potentially multiple routers for the same
// up data frame.
//
//An upinfo can be linked to its corresponding updf message via the SessID and FCntUp fields. Note: The value of upid is unique for each
//updf and upinfo messages and therefore does NOT link the upinfo message to a updf message.
type UpInfoTracknet struct {
	UpDfTracknet
	Upinfo []UpInfoElement `json:"upinfo"`
}

// UpInfoElement type element of UpInfo
type UpInfoElement struct {
	ArrTime     float64 `json:"ArrTime" gorethink:"ArrTime"`
	RX1DRoff    int64   `json:"RX1DRoff" gorethink:"RX1DRoff"` // expected type
	RxDelay     int64   `json:"RxDelay" gorethink:"RxDelay"`   // expected type
	DoorID      int64   `json:"doorid" gorethink:"doorid"`     // expected type
	MuxID       int64   `json:"muxid" gorethink:"muxid"`
	RegionID    int64   `json:"regionid" gorethink:"regionid"` // expected type
	RouterID    int64   `json:"routerid" gorethink:"routerid"`
	RouterIDStr string  `json:"routerid_str" gorethink:"routerid_str"`
	RSSI        float64 `json:"rssi" gorethink:"rssi"`
	RTT         []int16 `json:"rtt" gorethink:"rtt"`
	RxTime      float64 `json:"rxtime" gorethink:"rxtime"` // expected type
	SNR         float64 `json:"snr" gorethink:"snr"`
	Xtime       float64 `json:"xtime" gorethink:"xtime"` // expected type
}

/*
{
	"msgtype": 	"dntxed"
	"MsgId": 	INT8
	"upinfo": {
		"routerid": INT8 // identifier of the router which sent the down frame
	}
	"confirm": 	bool // confirmed down data frame?
	"DevEUI": 	EUI64 // device EUI-64
}
*/

// DnTxedTracknet type
// Reports successful transmission of a down data frame. The MsgId field links the dntxed message to the corresponding dndf message.
// Note that for confirmed down data frames every retransmission is reported separately until eventually acknowledged by the device, or until the
// buffered downstream message is deleted or replaced by the application.
type DnTxedTracknet struct {
	MsgType string `json:"msgtype"`
	MsgID   int64  `json:"MsgId"`
	UpInfo  struct {
		RouterID int64 `json:"routerid"`
	} `json:"upinfo"`
	Confirm bool   `json:"confirm"`
	DevEui  string `json:"DevEui"`
}

/*
{
	"msgtype": 	"dnacked"
	"MsgId": 	INT8
}
*/

// DnAckedTracknet type
// Reports acknowledgement of reception of a given down data frame by the device. The downstream message must have requested a confirmed down
// data frame.
// Note that for confirmed down data frames every retransmission is reported separately until eventually acknowledged by the device, or until the
// buffered downstream message is deleted or replaced by the application.
type DnAckedTracknet struct {
	MsgType string `json:"msgtype"`
	MsgID   int64  `json:"MsgId"`
}

/*
{
	"msgtype": 	"joining"
	"SessID": 	UINT4 			// unique session identifier per device
	"NetID": 	UINT4 			// LoRaWAN network identifier
	"DevEui": 	EUI64 			// device identifier
	"DR": 		int 			// data rate the message was sent with
	"Freq":		int 			// frequency the message was sent with
	"region":	str 			// region specifier
	"upinfo": 	[ UOBJ, .. ]	// see `updf` for details
}
*/

//JoiningTracknet type
// A joining message signals that a device, identified by the DevEui, initiated an over-the-air activation (OTAA) to the network.
type JoiningTracknet struct {
	MsgType string          `json:"msgtype"`
	SessID  int32           `json:"SessID"`
	NetID   int32           `json:"NetID"`
	DevEui  string          `json:"DevEui"`
	DR      int64           `json:"DR"`
	Freq    int64           `json:"Freq"`
	Region  string          `json:"region"`
	Upinfo  []UpInfoElement `json:"upinfo"`
}

// fake is a fake model, used as filtering match helper
// type fake struct {
// 	MsgType string `json:"msgtype"`
// 	DevEui  string `json:"DevEui"`
// }

// AppxMessage type
type AppxMessage struct {
	AppxURL string
	AppxID  string
	Message []byte
}

// var TCIOMessages = map[string]TCIOMessage{"dndf": &DnDfTracknet{}}

// type TCIOMessage interface {
// 	Unmarshal(tcMsgType string, rawMsg []byte) map[string]interface{}
// }

// func (msg *DnDfTracknet) Unmarshal(tcMsgType string, rawMsg []byte) map[string]interface{} {

// }

// FilterMessage func
func (ctx *Context) FilterMessage(message AppxMessage) bool {
	var fake map[string]interface{}
	//f := fake{}
	json.Unmarshal(message.Message, &fake)
	messagesRecievedByFilter.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL /*f.MsgType*/, fake["msgtype"].(string)).Inc()
	for _, allowedType := range ctx.Filters.MsgType {
		if /*f.MsgType*/ fake["msgtype"].(string) == allowedType || allowedType == "*" {
			for _, allowedDeveui := range ctx.CompilledFilters.ReExpressions {
				if allowedDeveui.MatchString( /*f.DevEui*/ fake["DevEui"].(string)) {
					messagesPassedFilter.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL /*f.MsgType*/, fake["msgtype"].(string)).Inc()
					logger.WithFields(log.Fields{"uri": message.AppxURL, "id": message.AppxID}).Debugf("%+v", message.Message)
					return true
				}
				messagesDroppedByDeveui.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL /*f.MsgType*/, fake["msgtype"].(string)).Inc()
			}
		} else {
			messagesDroppedByType.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL /*f.MsgType*/, fake["msgtype"].(string)).Inc()
		}
	}
	return false
}

// QueueProcessing func
func (ctx *Context) QueueProcessing(message <-chan AppxMessage, wg *sync.WaitGroup) {
	var buf []AppxMessage
	wggs.Add(1)
	var timeout = time.Duration(ctx.Owner.QueueFlushTime) * time.Millisecond
	flushTicker := time.NewTicker(timeout)
	for {
		select {
		case newMsg := <-message:
			buf = append(buf, newMsg)
			if len(buf) == ctx.Owner.QueueFlushCount {
				ctx.rethinkSink(buf)
				buf = nil
				break
			}
		case <-flushTicker.C:
			ctx.rethinkSink(buf)
			buf = nil
			flushTicker = time.NewTicker(timeout)
			break
		case <-shutdown:
			ctx.rethinkSink(buf)
			logger.Infof("QueueProcessing is terminating, flushing %v messages by final batch", len(buf))
			buf = nil
			flushTicker.Stop()
			wggs.Done()
			return
		}
	}
}

// dummysink
func (ctx *Context) dummysink(batch []AppxMessage) {
	var event interface{}
	for idx, msg := range batch {
		if ok := ctx.FilterMessage(msg); ok {
			json.Unmarshal(msg.Message, &event)
			log.Infof("%v, %v=>%v <%+v>", idx, msg.AppxID, msg.AppxURL, event)
		}
	}
}

// rethinkSink
/*
func (ctx *Context) rethinkSink(queue []AppxMessage) {
	var event interface{}
	var batch []interface{}
	r := re.DB("lora").Table("gpstracker")
	for idx, msg := range queue {
		if ok := ctx.FilterMessage(msg); ok {
			json.Unmarshal(msg.Message, &event)
			batch = append(batch, event)
			log.Infof("%v, %v=>%v <%+v>", idx, msg.AppxID, msg.AppxURL, event)
		}
	}
	_, err := r.Insert(batch).RunWrite(ctx.reSession)
	if err != nil {
		log.Warn("error insert to db")
	}
}
*/

// SinkQueue func
func (ctx *Context) SinkQueue(queue []AppxMessage) {
	var batch []interface{}
	for _, msg := range queue {
		if ok := ctx.FilterMessage(msg); ok {
			var event map[string]interface{}
			json.Unmarshal(msg.Message, &event)

			// fixing buggy trackcentral time handling and convert epoch to Time obj
			if _, ok := event["ArrTime"]; ok {
				event["ArrTime"] = time.Unix(int64(event["ArrTime"].(float64)), 0)
			} else {
				event["ArrTime"] = time.Now()
				event["TimeMissing"] = true
			}
			// try to decode, if fail - leave message as is
			event["DCDPayload"] = ctx.DecodePayload(event["DevEui"].(string), event["FRMPayload"].(string))
			batch = append(batch, event)
		}
	}
	logger.Infof("%d message(s) hit the sink, %d passed to decoders", len(queue), len(batch))

	for _, storage := range ctx.Owner.StoragePrefList {
		switch storage {
		case "rethinkdb":
			//go ctx.rethinkSink(batch)
			break
		case "elastic":
			break
		case "mqtt":
			break
		case "mongo":
			break
		default:
			logger.Fatalf("Unknown storage driver in config %s", storage)
		}
	}

}

func (ctx *Context) rethinkSink(queue []AppxMessage) {
	//var test UpDfTracknet
	var upinfos []UpInfoElement
	var batch []interface{}
	ctx.CheckRethinkAlive()
	r := re.DB(ctx.RethinkDB.DB).Table(ctx.RethinkDB.Collection)
	for idx, msg := range queue {
		var event map[string]interface{}
		if ok := ctx.FilterMessage(msg); ok {
			json.Unmarshal(msg.Message, &event)
			logger.Debugf("%v, %v=>%v <%+v>", idx, msg.AppxID, msg.AppxURL, event)
			if _, ok := event["ArrTime"]; ok {
				event["ArrTime"] = time.Unix(int64(event["ArrTime"].(float64)), 0)
			} else {
				event["ArrTime"] = time.Now()
				event["TimeMissing"] = true
			}
			event["DCDPayload"] = ctx.DecodePayload(event["DevEui"].(string), event["FRMPayload"].(string))
			// ugly tracknet/rethinkdb/nodejs biginteger fix
			if _, ok := event["upinfo"]; ok {
				for _, upinfo := range event["upinfo"].([]interface{}) {
					tmpb, err := json.Marshal(upinfo)
					if err != nil {
						logger.Panic(err)
					}

					var tmp UpInfoElement
					err = json.Unmarshal(tmpb, &tmp)
					if err != nil {
						logger.Panic(err)
					}
					tmp.RouterIDStr = fmt.Sprint(tmp.RouterID)
					upinfos = append(upinfos, tmp)
					logger.Debugf("rid-int: %d, rid-str: %s, rssi: %+v", tmp.RouterID, tmp.RouterIDStr, tmp.RSSI)
				}
				event["upinfo"] = upinfos
			}
			batch = append(batch, event)
		}
	}
	_, err := r.Insert(batch).RunWrite(ctx.reSession)
	if err != nil {
		log.Warn("error insert to db")
	}
}

// DecodePayload func
func (ctx *Context) DecodePayload(deveui string, payload string) interface{} {
	if devType, ok := ctx.Inventory[deveui]; ok {
		switch devType {
		case "TN-T0004001":
			payload, err := DecodeTNGps(payload)
			if err != nil {
				logger.WithFields(log.Fields{"DevEui": deveui, "type": devType, "payload": payload}).Errorf("Error decoding %+v", err)
			}
			return payload
		default:
			return nil
		}
	}
	return nil
}

// bigIntWorkaround func
func upInfoWorkaround(event map[string]interface{}) {

	var upinfos []UpInfoElement
	if _, ok := event["upinfo"]; ok {
		for _, upinfo := range event["upinfo"].([]interface{}) {
			tmpb, err := json.Marshal(upinfo)
			if err != nil {
				logger.Panic(err)
			}

			var tmp UpInfoElement
			err = json.Unmarshal(tmpb, &tmp)
			if err != nil {
				logger.Panic(err)
			}
			tmp.RouterIDStr = fmt.Sprint(tmp.RouterID)
			upinfos = append(upinfos, tmp)
			logger.Debugf("rid-int: %d, rid-str: %s, rssi: %+v", tmp.RouterID, tmp.RouterIDStr, tmp.RSSI)
		}
		event["upinfo"] = upinfos
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

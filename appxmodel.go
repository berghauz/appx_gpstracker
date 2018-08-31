package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	re "gopkg.in/gorethink/gorethink.v4"
	elastic "gopkg.in/olivere/elastic.v5"
)

// generic types for smooth rethinkdb edges like lack of big int support

// Time type
type Time struct {
	time.Time
}

// SetDefault func
func (t *Time) SetDefault() {
	t.Time = time.Now().Truncate(time.Second)
}

// BigInt type
// type BigInt struct {
// 	string
// }

type BigInt string

func (b BigInt) Set(value string) {
	b = BigInt(value)
}

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

// TracknetDnDfMsg type
// Submits a downstream message for transmission within the next downlink window for a given device.
type TracknetDnDfMsg struct {
	MsgType    string `json:"msgtype,omitempty"`
	MsgID      BigInt `json:"MsgId,omitempty"`
	FPort      uint8  `json:"FPort,omitempty"`
	FRMPayload string `json:"FRMPayload,omitempty"`
	DevEui     string `json:"DevEui,omitempty"`
	Confirm    bool   `json:"confirm,omitempty"`
	ArrTime    Time   `json:"ArrTime,omitempty"` // there is no such field in original tcio message, but for convenience' sake we add it
	SynthTime  bool   `json:"SinthTime,omitempty"`
}

// TracknetDnDfSpecialMsg type
type TracknetDnDfSpecialMsg struct {
	MsgType    string `json:"msgtype,omitempty"`
	MsgID      int64  `json:"MsgId,omitempty"`
	FPort      uint8  `json:"FPort,omitempty"`
	FRMPayload string `json:"FRMPayload,omitempty"`
	DevEui     string `json:"DevEui,omitempty"`
	Confirm    bool   `json:"confirm,omitempty"`
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

// TracknetUpDfMsg type
// Reports an upstream message with its up data frame payload as transmitted by a given device and additional context information. This message is
// forwarded without delay and, if a given up data frame is received by multiple gateways, only the first copy received is forwarded.
type TracknetUpDfMsg struct {
	MsgType    string `json:"msgtype,omitempty"`
	DevEui     string `json:"DevEui,omitempty"`
	UPID       BigInt `json:"upid,omitempty"`
	SessID     BigInt `json:"SessID,omitempty"`
	FCntUp     uint32 `json:"FCntUp,omitempty"`
	FPort      uint8  `json:"FPort,omitempty"`
	FRMPayload string `json:"FRMPayload,omitempty"`
	DR         uint32 `json:"DR,omitempty"`
	Freq       uint32 `json:"Freq,omitempty"`
	Region     string `json:"region,omitempty"`
	ArrTime    Time   `json:"ArrTime,omitempty"` // there is no such field in original tcio message, but for convenience' sake we add it
	SynthTime  bool   `json:"SinthTime,omitempty"`
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

// TracknetUpInfoMsg type
// Reports additional context information about an up data frame as received from a given device. This information usually arrives a short time after a
// corresponding updf message because forwarding is delayed to take into account context information from potentially multiple routers for the same
// up data frame.
//
//An upinfo can be linked to its corresponding updf message via the SessID and FCntUp fields. Note: The value of upid is unique for each
//updf and upinfo messages and therefore does NOT link the upinfo message to a updf message.
type TracknetUpInfoMsg struct {
	TracknetUpDfMsg
	UpInfo []TracknetUpInfoElement `json:"upinfo,omitempty"`
}

// TracknetUpInfoElement type element of UpInfo
type TracknetUpInfoElement struct {
	RouterID BigInt  `json:"routerid,omitempty" gorethink:"routerid,omitempty"`
	MuxID    BigInt  `json:"muxid,omitempty" gorethink:"muxid,omitempty"`
	RSSI     float64 `json:"rssi,omitempty" gorethink:"rssi,omitempty"`
	SNR      float64 `json:"snr,omitempty" gorethink:"snr,omitempty"`
	ArrTime  Time    `json:"ArrTime,omitempty" gorethink:"ArrTime,omitempty"`

	//RX1DRoff int64   `json:"RX1DRoff,omitempty" gorethink:"RX1DRoff,omitempty"` // expected type
	//RxDelay  int64   `json:"RxDelay,omitempty" gorethink:"RxDelay,omitempty"`   // expected type
	//DoorID   int64   `json:"doorid,omitempty" gorethink:"doorid,omitempty"`     // expected type
	//RegionID int64   `json:"regionid,omitempty" gorethink:"regionid,omitempty"` // expected type
	//RTT      []int16 `json:"rtt,omitempty" gorethink:"rtt,omitempty"`
	//RxTime   Date    `json:"rxtime,omitempty" gorethink:"rxtime,omitempty"` // expected type
	//Xtime    Date    `json:"xtime,omitempty" gorethink:"xtime,omitempty"`   // expected type
	//RouterIDStr string  `json:"routerid_str,omitempty" gorethink:"routerid_str,omitempty"`
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

// TracknetDnTxedMsg type
// Reports successful transmission of a down data frame. The MsgId field links the dntxed message to the corresponding dndf message.
// Note that for confirmed down data frames every retransmission is reported separately until eventually acknowledged by the device, or until the
// buffered downstream message is deleted or replaced by the application.
type TracknetDnTxedMsg struct {
	MsgType string `json:"msgtype"`
	MsgID   BigInt `json:"MsgId"`
	UpInfo  struct {
		RouterID BigInt `json:"routerid"`
	} `json:"upinfo"`
	Confirm   bool   `json:"confirm"`
	DevEui    string `json:"DevEui"`
	ArrTime   Time   `json:"ArrTime,omitempty"` // there is no such field in original tcio message, but for convenience' sake we add it
	SynthTime bool   `json:"SinthTime,omitempty"`
}

/*
{
	"msgtype": 	"dnacked"
	"MsgId": 	INT8
}
*/

// TracknetDnAckedMsg type
// Reports acknowledgement of reception of a given down data frame by the device. The downstream message must have requested a confirmed down
// data frame.
// Note that for confirmed down data frames every retransmission is reported separately until eventually acknowledged by the device, or until the
// buffered downstream message is deleted or replaced by the application.
type TracknetDnAckedMsg struct {
	MsgType   string `json:"msgtype"`
	MsgID     BigInt `json:"MsgId"`
	ArrTime   Time   `json:"ArrTime,omitempty"` // there is no such field in original tcio message, but for convenience' sake we add it
	SynthTime bool   `json:"SinthTime,omitempty"`
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

//TracknetJoiningMsg type
// A joining message signals that a device, identified by the DevEui, initiated an over-the-air activation (OTAA) to the network.
type TracknetJoiningMsg struct {
	MsgType   string                  `json:"msgtype"`
	SessID    BigInt                  `json:"SessID"`
	NetID     BigInt                  `json:"NetID"`
	DevEui    string                  `json:"DevEui"`
	DR        uint32                  `json:"DR"`
	Freq      uint32                  `json:"Freq"`
	Region    string                  `json:"region"`
	UpInfos   []TracknetUpInfoElement `json:"upinfo"`
	ArrTime   Time                    `json:"ArrTime,omitempty"` // there is no such field in original tcio message, but for convenience' sake we add it
	SynthTime bool                    `json:"SinthTime,omitempty"`
}

/*
{
	"msgtype": "joined"
	"SessID": 	UINT4 // unique session identifier per device
	"NetID": 	UINT4 // LoRaWAN network identifier
	"DevEui": 	EUI64 // device identifier
}
*/

// TracknetJoinedMsg type
//A joined message signals that a device completed an over-the-air activation (OTAA) to the network. The device establised a new session with the network.
type TracknetJoinedMsg struct {
	MsgType   string `json:"msgtype"`
	SessID    BigInt `json:"SessID"`
	NetID     BigInt `json:"NetID"`
	DevEui    string `json:"DevEui"`
	ArrTime   Time   `json:"ArrTime,omitempty"` // there is no such field in original tcio message, but for convenience' sake we add it
	SynthTime bool   `json:"SinthTime,omitempty"`
}

/*
undocumented
{
	"msgtype":"bad_dndf",
	"MsgId":123412341523,
	"FPort":1,
	"DevEui":"30-36-32-31-5C-37-6E-16",
	"error":"Not owner of this device"
}
*/

// TracknetBadDnDfMsg type
type TracknetBadDnDfMsg struct {
	MsgType   string `json:"msgtype"`
	MsgID     BigInt `json:"MsgId,omitempty"`
	FPort     uint8  `json:"FPort,omitempty"`
	DevEui    string `json:"DevEui"`
	Error     string `json:"error"`
	ArrTime   Time   `json:"ArrTime,omitempty"` // there is no such field in original tcio message, but for convenience' sake we add it
	SynthTime bool   `json:"SinthTime,omitempty"`
}

/*
undocumented
{
	"msgtype":"dnclr",
	"MsgId":0,
	"DevEui":"80-7B-85-90-20-00-05-5A",
	"upid":52150931123728752
}
*/

// TracknetDnClrMsg type
type TracknetDnClrMsg struct {
	MsgType   string `json:"msgtype"`
	MsgID     BigInt `json:"MsgId,omitempty"`
	DevEui    string `json:"DevEui"`
	UPID      BigInt `json:"upid,omitempty"`
	ArrTime   Time   `json:"ArrTime,omitempty"` // there is no such field in original tcio message, but for convenience' sake we add it
	SynthTime bool   `json:"SinthTime,omitempty"`
}

// TrackNetMessage type
type TrackNetMessage struct {
	MsgType             string `json:"-"`
	DevEui              string `json:"-"`
	*TracknetDnDfMsg    `json:"dndf,omitempty"`
	*TracknetUpDfMsg    `json:"updf,omitempty"`
	*TracknetUpInfoMsg  `json:"upinfo,omitempty"`
	*TracknetDnTxedMsg  `json:"dntxed,omitempty"`
	*TracknetDnAckedMsg `json:"dnacked,omitempty"`
	*TracknetJoiningMsg `json:"joining,omitempty"`
	*TracknetJoinedMsg  `json:"joined,omitempty"`
	*TracknetBadDnDfMsg `json:"bad_dndf,omitempty"`
	*TracknetDnClrMsg   `json:"dnclr,omitempty"`
}

// AppxMessage type
type AppxMessage struct {
	AppxURL string
	AppxID  string
	Message []byte
}

// GetType func
func (m *TrackNetMessage) GetType() string {
	return m.MsgType
}

// GetDevEui func
func (m *TrackNetMessage) GetDevEui() string {
	return m.DevEui
}

// GetFRMPayload func
func (m *TrackNetMessage) GetFRMPayload() string {
	switch m.MsgType {
	case "updf":
		if m.TracknetUpDfMsg.FRMPayload != "" {
			return m.TracknetUpDfMsg.FRMPayload
		}
	case "upinfo":
		if m.TracknetUpInfoMsg.FRMPayload != "" {
			return m.TracknetUpInfoMsg.FRMPayload
		}
	case "dndf":
		if m.TracknetDnDfMsg.FRMPayload != "" {
			return m.TracknetDnDfMsg.FRMPayload
		}
	}
	return ""
}

// GetMessage func
func (m *TrackNetMessage) GetMessage() map[string]interface{} {

	switch m.MsgType {
	case "updf":
		// dumb shit, but we need the time for es in any possible way
		if m.TracknetUpDfMsg.ArrTime.Unix() < 0 {
			m.TracknetUpDfMsg.ArrTime.SetDefault()
			m.TracknetUpDfMsg.SynthTime = true
		}
		return StructToMap(m.TracknetUpDfMsg)
	case "upinfo":
		if m.TracknetUpInfoMsg.ArrTime.Unix() < 0 {
			m.TracknetUpInfoMsg.ArrTime.SetDefault()
			m.TracknetUpInfoMsg.SynthTime = true
		}
		return StructToMap(m.TracknetUpInfoMsg)
	case "dndf":
		if m.TracknetDnDfMsg.ArrTime.Unix() < 0 {
			m.TracknetDnDfMsg.ArrTime.SetDefault()
			m.TracknetDnDfMsg.SynthTime = true
		}
		return StructToMap(m.TracknetDnDfMsg)
	case "dntxed":
		if m.TracknetDnTxedMsg.ArrTime.Unix() < 0 {
			m.TracknetDnTxedMsg.ArrTime.SetDefault()
			m.TracknetDnTxedMsg.SynthTime = true
		}
		return StructToMap(m.TracknetDnTxedMsg)
	case "dnacked":
		if m.TracknetDnAckedMsg.ArrTime.Unix() < 0 {
			m.TracknetDnAckedMsg.ArrTime.SetDefault()
			m.TracknetDnAckedMsg.SynthTime = true
		}
		return StructToMap(m.TracknetDnAckedMsg)
	case "joining":
		if m.TracknetJoiningMsg.ArrTime.Unix() < 0 {
			m.TracknetJoiningMsg.ArrTime.SetDefault()
			m.TracknetJoiningMsg.SynthTime = true
		}
		return StructToMap(m.TracknetJoiningMsg)
	case "joined":
		if m.TracknetJoinedMsg.ArrTime.Unix() < 0 {
			m.TracknetJoinedMsg.ArrTime.SetDefault()
			m.TracknetJoinedMsg.SynthTime = true
		}
		return StructToMap(m.TracknetJoinedMsg)
	case "bad_dndf":
		if m.TracknetBadDnDfMsg.ArrTime.Unix() < 0 {
			m.TracknetBadDnDfMsg.ArrTime.SetDefault()
			m.TracknetBadDnDfMsg.SynthTime = true
		}
		return StructToMap(m.TracknetBadDnDfMsg)
	case "dnclr":
		if m.TracknetDnClrMsg.ArrTime.Unix() < 0 {
			m.TracknetDnClrMsg.ArrTime.SetDefault()
			m.TracknetDnClrMsg.SynthTime = true
		}
		return StructToMap(m.TracknetDnClrMsg)
	}
	return nil
}

// StructToMap func
func StructToMap(message interface{}) map[string]interface{} {

	var tmp map[string]interface{}
	bytes, err := json.Marshal(message)
	if err != nil {
		logger.Warnf("Can't marshal %+v", message)
	}
	if err = json.Unmarshal(bytes, &tmp); err != nil {
		logger.Warnf("Can't unmarshal %+v", bytes)
	}
	return tmp
}

// UnmarshalJSON func
func (b *BigInt) UnmarshalJSON(data []byte) error {
	var i int64

	if err := json.Unmarshal(data, &i); err != nil {
		return err
	}

	*b = BigInt(fmt.Sprintf("%d", i))
	//fmt.Println(*b)
	return nil
}

// UnmarshalJSON func
func (t *Time) UnmarshalJSON(data []byte) error {
	var i float64
	if err := json.Unmarshal(data, &i); err != nil {
		return err
	}
	sec, dec := math.Modf(i)
	t.Time = time.Unix(int64(sec), int64(dec*( /*1e9*/ 0)))
	return nil
}

// UnmarshalJSON interface realisation for tracknet message type
func (m *TrackNetMessage) UnmarshalJSON(data []byte) error {
	temp := struct {
		MsgType string `json:"msgtype"`
	}{}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	switch temp.MsgType {
	case "updf":
		var tmpMsg TracknetUpDfMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetUpDfMsg = &tmpMsg
		m.MsgType = temp.MsgType
		m.DevEui = tmpMsg.DevEui
	case "upinfo":
		var tmpMsg TracknetUpInfoMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetUpInfoMsg = &tmpMsg
		m.MsgType = temp.MsgType
		m.DevEui = tmpMsg.DevEui
	case "dndf":
		var tmpMsg TracknetDnDfMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetDnDfMsg = &tmpMsg
		m.MsgType = temp.MsgType
		m.DevEui = tmpMsg.DevEui
	case "dntxed":
		var tmpMsg TracknetDnTxedMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetDnTxedMsg = &tmpMsg
		m.MsgType = temp.MsgType
		m.DevEui = tmpMsg.DevEui
	case "dnacked":
		var tmpMsg TracknetDnAckedMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetDnAckedMsg = &tmpMsg
		m.MsgType = temp.MsgType
		//m.DevEui = tmpMsg.DevEui
	case "joining":
		var tmpMsg TracknetJoiningMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetJoiningMsg = &tmpMsg
		m.MsgType = temp.MsgType
		m.DevEui = tmpMsg.DevEui
	case "joined":
		var tmpMsg TracknetJoinedMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetJoinedMsg = &tmpMsg
		m.MsgType = temp.MsgType
		m.DevEui = tmpMsg.DevEui
	case "bad_dndf":
		var tmpMsg TracknetBadDnDfMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetBadDnDfMsg = &tmpMsg
		m.MsgType = temp.MsgType
		m.DevEui = tmpMsg.DevEui
	case "dnclr":
		var tmpMsg TracknetDnClrMsg
		if err := json.Unmarshal(data, &tmpMsg); err != nil {
			return err
		}
		m.TracknetDnClrMsg = &tmpMsg
		m.MsgType = temp.MsgType
		m.DevEui = tmpMsg.DevEui
	default:
		return fmt.Errorf("Unknown tracknet message type: %s, %+v", temp.MsgType, string(data))
	}

	return nil
}

// FilterMessage func

// func (ctx *Context) FilterMessage(message AppxMessage) bool {
// 	var fake map[string]interface{}
// 	//f := fake{}
// 	json.Unmarshal(message.Message, &fake)
// 	messagesRecievedByFilter.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL /*f.MsgType*/, fake["msgtype"].(string)).Inc()
// 	for _, allowedType := range ctx.Filters.MsgType {
// 		if /*f.MsgType*/ fake["msgtype"].(string) == allowedType || allowedType == "*" {
// 			for _, allowedDeveui := range ctx.CompilledFilters.ReExpressions {
// 				if allowedDeveui.MatchString( /*f.DevEui*/ fake["DevEui"].(string)) {
// 					messagesPassedFilter.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL /*f.MsgType*/, fake["msgtype"].(string)).Inc()
// 					logger.WithFields(log.Fields{"uri": message.AppxURL, "id": message.AppxID}).Debugf("%+v", message.Message)
// 					return true
// 				}
// 				messagesDroppedByDeveui.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL /*f.MsgType*/, fake["msgtype"].(string)).Inc()
// 			}
// 		} else {
// 			messagesDroppedByType.WithLabelValues(ctx.AppName, message.AppxID, message.AppxURL /*f.MsgType*/, fake["msgtype"].(string)).Inc()
// 		}
// 	}
// 	return false
// }

// FilterMessage func
func (ctx *Context) FilterMessage(message *TrackNetMessage) bool {
	for _, allowedType := range ctx.Filters.MsgType {
		if message.MsgType == allowedType || allowedType == "*" {
			for _, allowedDeveui := range ctx.CompilledFilters.ReExpressions {
				if allowedDeveui.MatchString( /*f.DevEui*/ message.DevEui) {
					return true
				}
			}
		}
	}
	logger.Debugf("Dropped %s from %s", message.MsgType, message.DevEui)
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
			// think about => instead
			if len(buf) == ctx.Owner.QueueFlushCount {
				go ctx.SinkQueue(buf)
				buf = nil
				break
			}
		case <-flushTicker.C:
			if len(buf) > 0 {
				go ctx.SinkQueue(buf)
				buf = nil
			}
			break
		case <-shutdown:
			if len(buf) > 0 {
				logger.Infof("QueueProcessing is terminating, flushing %v messages by final batch", len(buf))
				ctx.SinkQueue(buf)
				buf = nil
			} else {
				logger.Infoln("QueueProcessing is terminating, nothing to flush.")
			}
			flushTicker.Stop()
			wggs.Done()
			return
		}
	}
}

// dummysink
func (ctx *Context) dummysink(batch []AppxMessage) {
	for _, msg := range batch {
		var event TrackNetMessage
		err := json.Unmarshal(msg.Message, &event)
		if err != nil {
			fmt.Println(err)
		}
		if ok := ctx.FilterMessage(&event); ok {
			//log.Infof("%v, %v=>%v <%+v>", idx, msg.AppxID, msg.AppxURL, event)
			//logger.Info(event.dummyPrint())
			event.dummyPrint()
			//Messages[event.MsgType].Get()
		}
	}
}

func (m *TrackNetMessage) dummyPrint() {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	logger.Debugln(string(b))

	// switch m.MsgType {
	// case "updf":
	// 	b, err := json.Marshal(m.TracknetUpDfMsg)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	logger.Println(string(b))
	// case "upinfo":
	// 	b, err := json.Marshal(m.TracknetUpInfoMsg)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	logger.Println(string(b))
	// }
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

	var (
		batch []interface{}
		msg   map[string]interface{}
	)

	for _, appxMsg := range queue {
		var event TrackNetMessage
		err := json.Unmarshal(appxMsg.Message, &event)
		if err != nil {
			logger.Errorf("Error unmarshaling upcoming message: %s", err)
			continue
		}
		if ok := ctx.FilterMessage(&event); ok {
			msg = event.GetMessage()
			switch event.MsgType {
			case "dndf":
				break
			case "dntxed":
				break
			case "dnacked":
				break
			case "joining":
				break
			case "joined":
				break
			case "bad_dndf":
				break
			case "dnclr":
				break
				//case "updf":
			//case "upinfo":
			default:
				if event.GetFRMPayload() != "" {
					payload := ctx.DecodePayload(event.GetDevEui() /*event.TracknetUpDfMsg.FRMPayload*/, event.GetFRMPayload())
					if payload != nil {
						msg["payload"] = &payload
					}
				}
			}
			batch = append(batch, msg)
		}
	}

	if len(batch) > 0 {
		b, _ := json.Marshal(batch)
		logger.Debugf("Upcoming message: %s", string(b))

		for _, storage := range ctx.Owner.StoragePrefList {
			switch storage {
			case "rethinkdb":
				ctx.rethinkSink(&batch)
			case "elastic":
				ctx.elasticSink(&batch)
			case "mqtt":
				ctx.mqttSink(&batch)
				break
			case "mongo":
				break
			default:
				logger.Fatalf("Unknown storage driver [%s] in config", storage)
			}
		}
	}
}

func (ctx *Context) rethinkSink(batch *[]interface{}) {

	ctx.CheckRethinkAlive()
	r := re.DB(ctx.RethinkDB.DB).Table(ctx.RethinkDB.Collection)
	_, err := r.Insert(batch).RunWrite(ctx.reSession)
	if err != nil {
		logger.Error("error insert to db")
	}
}

func (ctx *Context) elasticSink(batch *[]interface{}) {

	bulkRequest := ctx.esClient.Bulk()
	for _, each := range *batch {
		//fmt.Println(each)
		//id := uuid.NewV3(uuid.NamespaceURL, ctx.AppName)
		req := elastic.NewBulkIndexRequest().Index(time.Now().Format(ctx.Elastic.Index)).Type("logs"). /*.Id(id.String())*/ Doc(each)
		bulkRequest = bulkRequest.Add(req)
	}

	_, err := bulkRequest.Do(context.Background())
	if err != nil {
		logger.Errorf("ElasticSearch Do request error: %v", err)
	}
}

func (ctx *Context) mqttSink(batch *[]interface{}) {
	events, err := json.Marshal(*batch)
	if err != nil {
		log.Errorf("Can't convert batch to json string: %+v", err)
		return
	}
	// for _, event := range *batch {
	// if ctx.mqttClient.IsConnected() {
	if token := ctx.mqttClient.Publish(ctx.Mqtt.UpTopic, ctx.Mqtt.UpQoS, false, string(events)); token.Wait() && token.Error() != nil {
		logger.Errorf("Failed to publish to mqtt: %+v", token.Error())
	}
	// } else {
	// 	logger.Errorln("Failed to publish to mqtt: client not connected")
	// }
	// }
}

// handleMqttUpMessage
func (p *connPool) handleMqttDnMessage(c mqtt.Client, m mqtt.Message) {
	var dnMsg []TracknetDnDfSpecialMsg
	if err := json.Unmarshal(m.Payload(), &dnMsg); err != nil {
		logger.Errorf("Can't umrashall incoming dndf %s, %+v", string(m.Payload()), err)
	}
	logger.Infof("Downcoming message: %+v", dnMsg)
	for _, msg := range dnMsg {
		if msg.DevEui == "" || msg.MsgType != "dndf" {
			logger.Errorf("Incorrect incoming dndf message %s, dropping", string(m.Payload()))
		} else {
			var randConn *connection
			for conn := range p.connections {
				randConn = conn
				break
			}
			if err := randConn.ws.WriteJSON(msg); err != nil {
				logger.Errorf("Fail to send dn message %+v", err)
			}
		}
	}
	//logger.Infof("updn %+v %+v %+v %+v %+v %+v", m.Duplicate(), m.MessageID(), m.Qos(), m.Topic(), m.Retained(), string(m.Payload()))
}

// DecodePayload func
func (ctx *Context) DecodePayload(deveui string, payload string) interface{} {
	if devType, ok := ctx.Inventory[deveui]; ok {
		if decoder, ok := ctx.DecodingPlugins[devType]; ok {
			payload, err := decoder(payload)
			if err != nil {
				logger.WithFields(log.Fields{"DevEui": deveui, "type": devType}).Errorf("Error decoding %+v", err)
			}
			return payload
		} /*else {*/
		logger.WithFields(log.Fields{"DevEui": deveui, "type": devType, "payload": payload}).Errorln("Decored not found")
		//}
	} /*else {
		if len(ctx.Inventory) > 0 {
			logger.WithFields(log.Fields{"DevEui": deveui, "payload": payload}).Errorln("Device not listed in inventory")
		}
	}*/
	return nil
}

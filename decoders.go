package main

import (
	"encoding/binary"
	"encoding/hex"
)

type trackNetGps struct {
	DataType  byte
	DataSize  int
	ValueType string
	Next      int
}

/*
Data Channel (1-Byte) | Data Type (1-Byte) | Data (N-Bytes)
Type Information	Data Channel	Data Type	Data Size (byteData)	Data Type		Data Format
Battery Voltage		0x00			0xFF		2					Analog Input	0.01 V / LSB (Signed)
GPS Location		0x01			0x88		9					GPS Location	3-byteData Latitude/Longitude/Altitude (Lat/Long: 0.0001° Signed, Alt: 0.01 meter Signed )
GPS Status			0x02			0x00		1					Digital Input	Boolean
GPS Location HP*	0x03			0x89		8					GPS Location 	4-byteData Latitude/Longitude Lat/Long: 0.0000001°/LSB (Signed)
Impact Magnitude	0x05			0x02		2					Analog Input	0.01 Signed
Break In			0x06			0x00		1					Digital Input	Boolean
Accelerometer Data	0x07			0x71		6					Accelerometer	2-byteData X/Y/Z 0.01 g/LSB (Signed)
MCU Temperature		0x0B			0x67		2					Temperature		0.1 °C / LSB (Signed)
Impact Alarm		0x0C			0x00		1					Digital Input	Boolean

HP - High Precision
*/

// DecodeTNGps func
func DecodeTNGps(payload string) (interface{}, error) {

	var (
		decoded = make(map[string]interface{})
		offset  int
		format  = map[byte]trackNetGps{
			0x00: {0xff, 2, "battery", 4},
			0x01: {0x88, 9, "gps", 11},
		}
	)

	byteData, err := hex.DecodeString( /*"018808814705BF070052E400FF0152"*/ payload)
	if err != nil {
		logger.Errorf("Error conver payload %s to byte array %+v", payload, err)
		return nil, err
	}

	for {
		if val, ok := format[byteData[offset]]; ok {
			if byteData[offset+1] == val.DataType {
				switch val.ValueType {
				case "battery":
					tmp := byteData[offset+2 : offset+2+val.DataSize]
					bits := binary.BigEndian.Uint16(tmp)
					sign := bits&1 != 0
					if sign {
						decoded["battery"] = float64(bits) * -0.01
						logger.Printf("===battery %f", float64(bits)*-0.01)
					} else {
						decoded["battery"] = float64(bits) * 0.01
						logger.Printf("===battery %f", float64(bits)*0.01)
					}
				case "gps":
					tmp := byteData[offset+2 : offset+2+val.DataSize]
					lati := threeByteToInt64(tmp[:3])
					loni := threeByteToInt64(tmp[3:6])
					alti := threeByteToInt64(tmp[6:9])
					decoded["gps"] = []float64{float64(lati) * 0.0001, float64(loni) * 0.0001, float64(alti) * 0.01}
					logger.Printf("===gps %f, %f, %f", float64(lati)*0.0001, float64(loni)*0.0001, float64(alti)*0.01)
				}
				offset += val.Next
			}
		}
		if offset == len(byteData) {
			break
		}
	}
	return decoded, nil
}

func threeByteToInt64(value []byte) int {
	if len(value) != 3 {
		logger.Errorf("threeByteToInt64 got value not equal 3 bytes %+v", value)
		return 0
	}
	return int(uint(value[2]) | uint(value[1])<<8 | uint(value[0])<<16)
}

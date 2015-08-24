// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bytes"
	"log"
)

// Message
//
// The different types of messages that are exchanged between the server
// and the client include audio messages for sending the audio data,
// video messages for sending video data, data messages for sending any
// user data, shared object messages, and command messages.
type Message struct {
	Timestamp         uint32
	ChunkStreamID     uint32
	Size              uint32
	Type              uint8
	MessageStreamID uint32
	Buf               *bytes.Buffer
	IsInbound         bool
	AbsoluteTimestamp uint32
}

func NewMessage(csi uint32, t uint8, sid uint32, ts uint32, data []byte) *Message {
	message := &Message{
		Timestamp:         ts,
		ChunkStreamID:     csi,
		Type:              t,
		MessageStreamID:   sid,
		AbsoluteTimestamp: ts,
		Buf:               new(bytes.Buffer),
	}
	if data != nil {
		message.Buf.Write(data)
		message.Size = uint32(len(data))
	}
	return message
}

func (message *Message) Dump(name string) {
	direction := "outbound"
    if message.IsInbound {
		direction = "inbound"
	}
	log.Printf(
		"%s[cs %d] AMF3 Message: timestamp: %d, ms id: %d, cs id: %d, type: %d (%s), size: %d, %s, AbsoluteTimestamp: %d", 
		name, message.ChunkStreamID, message.Timestamp, message.MessageStreamID, message.ChunkStreamID, 
		message.Type, message.TypeDisplay(), message.Size, direction, message.AbsoluteTimestamp)
}

// The length of remain data to read
func (message *Message) Remain() uint32 {
	if message.Buf == nil {
		return message.Size
	}
	return message.Size - uint32(message.Buf.Len())
}

func (message *Message) TypeDisplay() string {
	switch (message.Type) {
		default:
			return "unknown"
		case 20:
			return "command-amf0"
		case 17:
			return "command-amf3"
		case 18:
			return "data-amf0"
		case 15:
			return "data-amf3"
		case 8:
			return "audio"
		case 9:
			return "video"
		case 22:
			return "aggregate"
	}
}


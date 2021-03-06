// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bytes"
	"log"
	"fmt"
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

func NewMessage(csid uint32, typ uint8, msid uint32, ts uint32, data []byte) *Message {
	message := &Message{
		Timestamp:         ts,
		ChunkStreamID:     csid,
		Type:              typ,
		MessageStreamID:   msid,
		AbsoluteTimestamp: ts,
		Buf:               new(bytes.Buffer),
	}
	if data != nil {
		message.Buf.Write(data)
		message.Size = uint32(len(data))
	}
	return message
}

func CopyToStream(stream ServerStream, messageIn *Message) *Message {
	byteCopy := make([]byte, messageIn.Buf.Len())
	copy(byteCopy, messageIn.Buf.Bytes())
	newBuffer := bytes.NewBuffer(byteCopy)

	msgOut := Message{
		Timestamp: messageIn.Timestamp,
		ChunkStreamID: stream.ChunkStreamID(),
		Size: messageIn.Size,
		Type: messageIn.Type,
		MessageStreamID: stream.ID(),
		Buf: newBuffer,
		IsInbound: false,
		AbsoluteTimestamp: messageIn.AbsoluteTimestamp,
	}
	
	return &msgOut
}

func (message *Message) Dump(name string) string {
	direction := "outbound"
    if message.IsInbound {
		direction = "inbound"
	}
	return fmt.Sprintf(
		"%s[cs %d] AMF3 Message: timestamp: %d, ms id: %d, cs id: %d, type: %d (%s), size: %d, %s, AbsoluteTimestamp: %d", 
		name, message.ChunkStreamID, message.Timestamp, message.MessageStreamID, message.ChunkStreamID, 
		message.Type, message.TypeDisplay(), message.Size, direction, message.AbsoluteTimestamp)
}
func (message *Message) LogDump(name string) {
	log.Println(message.Dump(name))
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
		case 1:
			return "set-chunk-size"
		case 2:
			return "abort"
		case 3:
			return "acknowledgment"
		case 4:
			return "user-control"
		case 5:
			return "window-size-ack"
		case 6:
			return "set-peer-bandwidth"
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


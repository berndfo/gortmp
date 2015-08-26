// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bytes"
	"fmt"
	"github.com/berndfo/goamf"
	"log"
)

type ServerStreamHandler interface {
	OnPlayStart(stream ServerStream) // TODO add play command parameters
	OnPublishStart(stream ServerStream, publishingName string, publishingType string)
	OnReceiveAudio(stream ServerStream, on bool)
	OnReceiveVideo(stream ServerStream, on bool)
	// TODO missing play2, deleteStream, seek, pause
}

// Message stream:
//
// A logical channel of communication that allows the flow of
// messages.
type serverStream struct {
	id            uint32
	streamName    string
	conn          *serverConn
	chunkStreamID uint32
	handler ServerStreamHandler
	bufferLength  uint32
}

// A RTMP logical stream on connection.
type ServerStream interface {
	Conn() ServerConn
	// ID
	ID() uint32
	// ChunkStreamID
	ChunkStreamID() uint32
	// StreamName
	StreamName() string
	// Close
	Close()
	// Received messages
	Received(message *Message) (handlered bool)
	// Attach handler
	Attach(handler ServerStreamHandler)
	// Send audio data
	SendAudioData(data []byte, deltaTimestamp uint32) error
	// Send video data
	SendVideoData(data []byte, deltaTimestamp uint32) error
	// Send data
	SendData(dataType uint8, data []byte, deltaTimestamp uint32) error
}

func (stream *serverStream) Conn() ServerConn {
	return stream.conn
}

// ID
func (stream *serverStream) ID() uint32 {
	return stream.id
}

// ChunkStreamID
func (stream *serverStream) ChunkStreamID() uint32 {
	return stream.chunkStreamID
}

// StreamName
func (stream *serverStream) StreamName() string {
	return stream.streamName
}

// Close
func (stream *serverStream) Close() {
	var err error
	cmd := &Command{
		IsFlex:        true,
		Name:          "closeStream",
		TransactionID: 0,
		Objects:       make([]interface{}, 1),
	}
	cmd.Objects[0] = nil
	message := NewMessage(stream.chunkStreamID, COMMAND_AMF3, stream.id, AUTO_TIMESTAMP, nil)
	if err = cmd.Write(message.Buf); err != nil {
		return
	}
	message.Dump("closeStream")
	conn := stream.conn.Conn()
	conn.Send(message)
}
            
func (stream *serverStream) Received(message *Message) bool {
	if (DebugLog) {
		log.Printf("[stream %d][cs %d] server received msg, type = %d(%s)", stream.id, stream.chunkStreamID, message.Type, message.TypeDisplay())
	}
	if message.Type == VIDEO_TYPE || message.Type == AUDIO_TYPE {
		return false
	}
	var err error
	if message.Type == COMMAND_AMF0 || message.Type == COMMAND_AMF3 {
		cmd := &Command{}
		if message.Type == COMMAND_AMF3 {
			cmd.IsFlex = true
			_, err = message.Buf.ReadByte()
			if err != nil {
				log.Printf("serverStream::Received() Read first in flex commad err:", err)
				return true
			}
		}
		cmd.Name, err = amf.ReadString(message.Buf)
		if err != nil {
			log.Printf("serverStream::Received() AMF0 Read name err:", err)
			return true
		}
		var transactionID float64
		transactionID, err = amf.ReadDouble(message.Buf)
		if err != nil {
			log.Printf("serverStream::Received() AMF0 Read transactionID err:", err)
			return true
		}
		cmd.TransactionID = uint32(transactionID)
		var object interface{}
		for message.Buf.Len() > 0 {
			object, err = amf.ReadValue(message.Buf)
			if err != nil {
				log.Printf("serverStream::Received() AMF0 Read object err:", err)
				return true
			}
			cmd.Objects = append(cmd.Objects, object)
		}

		log.Printf("identified server command: %q", cmd.Name)
		switch cmd.Name {
		case "play":
			return stream.onPlay(cmd)
		case "publish":
			return stream.onPublish(cmd)
		case "receiveAudio":
			return stream.onReceiveAudio(cmd)
		case "receiveVideo":
			return stream.onReceiveVideo(cmd)
		case "closeStream":
			return stream.onCloseStream(cmd)
		default:
			log.Printf("serverStream::Received: %+v\n", cmd)
		}

	}
	return false
}

func (stream *serverStream) Attach(handler ServerStreamHandler) {
	stream.handler = handler
}

// Send audio data
func (stream *serverStream) SendAudioData(data []byte, deltaTimestamp uint32) (err error) {
	message := NewMessage(stream.chunkStreamID-4, AUDIO_TYPE, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

// Send video data
func (stream *serverStream) SendVideoData(data []byte, deltaTimestamp uint32) (err error) {
	message := NewMessage(stream.chunkStreamID-4, VIDEO_TYPE, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

// Send data
func (stream *serverStream) SendData(dataType uint8, data []byte, deltaTimestamp uint32) (err error) {
	var csid uint32
	switch dataType {
	case VIDEO_TYPE:
		csid = stream.chunkStreamID - 4
	case AUDIO_TYPE:
		csid = stream.chunkStreamID - 4
	default:
		csid = stream.chunkStreamID
	}
	message := NewMessage(csid, dataType, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

func (stream *serverStream) onPlay(cmd *Command) bool {
	log.Printf("[stream %d][cs %d] onPlay", stream.ID(), stream.chunkStreamID)
	// Get stream name
	if cmd.Objects == nil || len(cmd.Objects) < 2 || cmd.Objects[1] == nil {
		log.Printf("serverStream::onPlay: command error 1! %+v\n", cmd)
		return true
	}

	if streamName, ok := cmd.Objects[1].(string); !ok {
		log.Printf("serverStream::onPlay: command error 2! %+v\n", cmd)
		return true
	} else {
		stream.streamName = streamName
	}
	// Response
	stream.conn.conn.SetChunkSize(4096)
	stream.conn.conn.SendUserControlMessage(EVENT_STREAM_BEGIN)
	stream.streamReset()
	stream.streamStart()
	stream.rtmpSampleAccess()
	stream.handler.OnPlayStart(stream)
	return true
}

func (stream *serverStream) onPublish(cmd *Command) bool {
	log.Printf("[stream %d][cs %d] onPublish", stream.ID(), stream.chunkStreamID)
	publishingName := "camera01"
	publishingType := "live"
	stream.handler.OnPublishStart(stream, publishingName, publishingType)
	return true
}
func (stream *serverStream) onReceiveAudio(cmd *Command) bool {
	log.Printf("[stream %d][cs %d] onReceiveAudio", stream.ID(), stream.chunkStreamID)
	// TODO parse command parameter "Bool flag"
	requestingData := true
	stream.handler.OnReceiveAudio(stream, requestingData)
	return true
}
func (stream *serverStream) onReceiveVideo(cmd *Command) bool {
	log.Printf("[stream %d][cs %d] onReceiveAudio", stream.ID(), stream.chunkStreamID)
	// TODO parse command parameter "Bool flag"
	requestingData := true
	stream.handler.OnReceiveVideo(stream, requestingData)
	return true
}
func (stream *serverStream) onCloseStream(cmd *Command) bool {
	log.Printf("[stream %d][cs %d] onCloseStream **empty**", stream.ID(), stream.chunkStreamID)
	return true
}

func (stream *serverStream) streamReset() {
	cmd := &Command{
		IsFlex:        false,
		Name:          "onStatus",
		TransactionID: 0,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = amf.Object{
		"level":       "status",
		"code":        NETSTREAM_PLAY_RESET,
		"description": fmt.Sprintf("playing and resetting %s", stream.streamName),
		"details":     stream.streamName,
	}
	buf := new(bytes.Buffer)
	err := cmd.Write(buf)
	CheckError(err, "serverStream::streamReset() Create command")

	message := &Message{
		ChunkStreamID: CS_ID_USER_CONTROL,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump("streamReset")
	stream.conn.conn.Send(message)
}

func (stream *serverStream) streamStart() {
	cmd := &Command{
		IsFlex:        false,
		Name:          "onStatus",
		TransactionID: 0,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = amf.Object{
		"level":       "status",
		"code":        NETSTREAM_PLAY_START,
		"description": fmt.Sprintf("Started playing %s", stream.streamName),
		"details":     stream.streamName,
	}
	buf := new(bytes.Buffer)
	err := cmd.Write(buf)
	CheckError(err, "serverStream::streamStart() Create command")

	message := &Message{
		ChunkStreamID: CS_ID_USER_CONTROL,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump("streamStart")
	stream.conn.conn.Send(message)
}

func (stream *serverStream) rtmpSampleAccess() {
	message := NewMessage(CS_ID_USER_CONTROL, DATA_AMF0, 0, 0, nil)
	amf.WriteString(message.Buf, "|RtmpSampleAccess")
	amf.WriteBoolean(message.Buf, false)
	amf.WriteBoolean(message.Buf, false)
	message.Dump("rtmpSampleAccess")
	stream.conn.conn.Send(message)
}

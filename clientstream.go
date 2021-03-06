// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"errors"
	"github.com/berndfo/goamf"
	"log"
)

type ClientStreamHandler interface {
	OnPlayStart(stream ClientStream)
	OnPublishStart(stream ClientStream)
}

// Message stream:
//
// A logical channel of communication that allows the flow of
// messages.
type clientStream struct {
	id            uint32
	conn ClientConn
	chunkStreamID uint32
	handler ClientStreamHandler
	bufferLength  uint32
}

// A RTMP logical stream, client-side view
type ClientStream interface {
	ClientPublishStream
	ClientPlayStream
	// ID
	ID() uint32
	// Pause
	Pause() error
	// Resume
	Resume() error
	// Close
	Close()
	// Received messages
	Received(message *Message) (handlered bool)
	// Attach handler
	Attach(handler ClientStreamHandler)
	// Publish audio data
	PublishAudioData(data []byte, deltaTimestamp uint32) error
	// Publish video data
	PublishVideoData(data []byte, deltaTimestamp uint32) error
	// Publish data
	PublishData(dataType uint8, data []byte, deltaTimestamp uint32) error
	// Call
	Call(name string, customParameters ...interface{}) error
}

// A publish stream
type ClientPublishStream interface {
	// Publish
	Publish(name, t string) (err error)
	// Send audio data
	SendAudioData(data []byte) error
	// Send video data
	SendVideoData(data []byte) error
}

// A play stream
type ClientPlayStream interface {
	// Play
	Play(streamName string, start, duration float32, reset bool) (err error)
	// Seeks the kerframe closedst to the specified location.
	Seek(offset uint32)
}

// ID
func (stream *clientStream) ID() uint32 {
	return stream.id
}

// Pause
func (stream *clientStream) Pause() error {
	return errors.New("Unimplemented")
}

// Resume
func (stream *clientStream) Resume() error {
	return errors.New("Unimplemented")
}

// Close
func (stream *clientStream) Close() {
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
	message.LogDump("closeStream")
	conn := stream.conn.Conn()
	conn.Send(message)
}

// Send audio data
func (stream *clientStream) SendAudioData(data []byte) error {
	return errors.New("Unimplemented")
}

// Send video data
func (stream *clientStream) SendVideoData(data []byte) error {
	return errors.New("Unimplemented")
}

// Seeks the frame closest to the specified location.
func (stream *clientStream) Seek(offset uint32) {}

func (stream *clientStream) Publish(streamName, howToPublish string) (err error) {
	conn := stream.conn.Conn()
	// Create publish command
	cmd := &Command{
		IsFlex:        true,
		Name:          "publish",
		TransactionID: 0,
		Objects:       make([]interface{}, 3),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = streamName
	if len(howToPublish) > 0 {
		cmd.Objects[2] = howToPublish
	} else {
		cmd.Objects[2] = nil
	}

	// Construct message
	message := NewMessage(stream.chunkStreamID, COMMAND_AMF3, stream.id, 0, nil)
	if err = cmd.Write(message.Buf); err != nil {
		return
	}
	message.LogDump("publish")

	return conn.Send(message)
}

func (stream *clientStream) Play(streamName string, start, duration float32, reset bool) (err error) {
	conn := stream.conn.Conn()
	// Keng-die: in stream transaction ID always been 0
	// Create play command
	cmd := &Command{
		IsFlex:        false,
		Name:          "play",
		TransactionID: 0,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = streamName
	cmd.Objects = append(cmd.Objects, start)
	
	cmd.Objects = append(cmd.Objects, duration)
	cmd.Objects = append(cmd.Objects, reset)

	// Construct message
	message := NewMessage(stream.chunkStreamID, COMMAND_AMF0, stream.id, 0, nil)
	if err = cmd.Write(message.Buf); err != nil {
		return
	}
	message.LogDump("play")

	err = conn.Send(message)
	if err != nil {
		return
	}

	// Set Buffer Length
	// Buffer length
	if stream.bufferLength < MIN_BUFFER_LENGTH {
		stream.bufferLength = MIN_BUFFER_LENGTH
	}
	stream.conn.Conn().SetStreamBufferSize(stream.id, stream.bufferLength)
	return nil
}

func (stream *clientStream) Call(name string, customParameters ...interface{}) (err error) {
	conn := stream.conn.Conn()
	// Create play command
	cmd := &Command{
		IsFlex:        false,
		Name:          name,
		TransactionID: 0,
		Objects:       make([]interface{}, 1+len(customParameters)),
	}
	cmd.Objects[0] = nil
	for index, param := range customParameters {
		cmd.Objects[index+1] = param
	}

	// Construct message
	message := NewMessage(stream.chunkStreamID, COMMAND_AMF0, stream.id, 0, nil)
	if err = cmd.Write(message.Buf); err != nil {
		return
	}
	message.LogDump(name)

	err = conn.Send(message)
	if err != nil {
		return
	}

	// Set Buffer Length
	// Buffer length
	if stream.bufferLength < MIN_BUFFER_LENGTH {
		stream.bufferLength = MIN_BUFFER_LENGTH
	}
	stream.conn.Conn().SetStreamBufferSize(stream.id, stream.bufferLength)
	return nil
}

func (stream *clientStream) Received(message *Message) bool {
	log.Printf("[stream %d][cs %d] client received msg, type = %d(%s)", stream.id, stream.chunkStreamID, message.Type, message.TypeDisplay())
	if message.Type == VIDEO_TYPE || message.Type == AUDIO_TYPE {
		return false
	}
	var err error
	if message.Type == COMMAND_AMF0 || message.Type == COMMAND_AMF3 {
		// Netstream commands start with: 
		// 1. Command Name : String
		// 2. Transaction ID : Number
		// 3. Object : NULL Type
		
		cmd := &Command{}
		if message.Type == COMMAND_AMF3 {
			cmd.IsFlex = true
			_, err = message.Buf.ReadByte()
			if err != nil {
				log.Println("clientStream::Received() Read first in flex commad err:", err)
				return true
			}
		}
		
		cmd.Name, err = amf.ReadString(message.Buf)
		if err != nil {
			log.Println("clientStream::Received() AMF0 Read name err:", err)
			return true
		}

		var transactionID float64
		transactionID, err = amf.ReadDouble(message.Buf)
		if err != nil {
			log.Println("clientStream::Received() AMF0 Read transactionID err:", err)
			return true
		}
		cmd.TransactionID = uint32(transactionID)
		
		var objectNil interface{}
		objectNil, err = amf.ReadValue(message.Buf)
		if err != nil {
			log.Println("clientStream::Received() AMF0 Read Command Object err:", err)
			return true
		}
		if objectNil != nil {
			log.Println("clientStream::Received() AMF0 Read Command Object must be NULL err:", err)
			return true
		}
		
		var object interface{}
		log.Printf("client message received: buffer len = %d", message.Buf.Len())
		for message.Buf.Len() > 0 {
			object, err = amf.ReadValue(message.Buf)
			if err != nil {
				log.Println("clientStream::Received() AMF0 Read object err:", err)
				return true
			}
			cmd.Objects = append(cmd.Objects, object)
		}
		switch cmd.Name {
		case "onStatus":
			return stream.onStatus(cmd)
		case "onMetaData":
			return stream.onMetaData(cmd)
		case "onTimeCoordInfo":
			return stream.onTimeCoordInfo(cmd)
		default:
			log.Printf("clientStream::Received() Unknown command: %s\n", cmd.Name)
		}
	}
	return false
}

func (stream *clientStream) onStatus(cmd *Command) bool {
	log.Printf("onStatus: %+v, objects = %d", cmd, len(cmd.Objects))
	code := ""
	objMap := cmd.Objects[0].(amf.Object)
	if len(objMap) >= 2 {
		level := objMap["level"].(string)
		code = objMap["code"].(string)
		log.Printf("level: %s, code: %s", level, code)
	}
	log.Printf("onStatus: parsed code = %q", code)
	switch code {
	case NETSTREAM_PLAY_START:
		log.Println("Play started")
		// Set buffer size
		//stream.conn.Conn().SetStreamBufferSize(stream.id, 1500)
		if stream.handler != nil {
			stream.handler.OnPlayStart(stream)
			return true
		}
	case NETSTREAM_PUBLISH_START:
		log.Println("Publish started")
		if stream.handler != nil {
			stream.handler.OnPublishStart(stream)
			return true
		}
	}
	return false
}

func (stream *clientStream) onMetaData(cmd *Command) bool {
	cmd.LogDump("onMetaData")
	return false
}

func (stream *clientStream) onTimeCoordInfo(cmd *Command) bool {
	cmd.LogDump("onTimeCoordInfo")
	return false
}

func (stream *clientStream) Attach(handler ClientStreamHandler) {
	stream.handler = handler
}

// Publish audio data
func (stream *clientStream) PublishAudioData(data []byte, deltaTimestamp uint32) (err error) {
	message := NewMessage(stream.chunkStreamID, AUDIO_TYPE, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

// Publish video data
func (stream *clientStream) PublishVideoData(data []byte, deltaTimestamp uint32) (err error) {
	message := NewMessage(stream.chunkStreamID, VIDEO_TYPE, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

// Publish data
func (stream *clientStream) PublishData(dataType uint8, data []byte, deltaTimestamp uint32) (err error) {
	message := NewMessage(stream.chunkStreamID, dataType, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

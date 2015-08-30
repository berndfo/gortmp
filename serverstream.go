// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bytes"
	"fmt"
	"github.com/berndfo/goamf"
	"log"
	"sync"
)

type ServerStreamHandler interface {
	OnPlayStart(stream ServerStream, name string, start float64, duration float64, flushPrevPlaylist bool)
	OnPublishStart(stream ServerStream, publishingName string, publishingType string)
	// client asks to start/stop receiving audio
	OnReceiveAudio(stream ServerStream, on bool)
	// client asks to start/stop receiving video
	OnReceiveVideo(stream ServerStream, on bool)

	// TODO missing play2, deleteStream, seek, pause

	// client sends audio stream data
	OnAudioData(stream ServerStream, audio *Message)
	// client sends video stream data
	OnVideoData(stream ServerStream, video *Message)
}

// A RTMP logical stream, server-side view
type ServerStream interface {
	Conn() ServerConn
	// ID
	ID() uint32
	// ChunkStreamID
	ChunkStreamID() uint32
	// StreamName
	StreamName() string
	SetStreamName(string)

	Handlers() []ServerStreamHandler

	// Close
	Close()
	// Received messages
	StreamMessageReceiver() (chan<- *Message)
	// Attach handler
	Attach(handler ServerStreamHandler) []ServerStreamHandler
	// Send audio data
	SendAudioData(data []byte, deltaTimestamp uint32) error
	// Send video data
	SendVideoData(data []byte, deltaTimestamp uint32) error
	// Send data
	SendData(dataType uint8, data []byte, deltaTimestamp uint32) error
}

// Message stream:
//
// A logical channel of communication that allows the flow of
// messages.
// implements ServerStream
type serverStream struct {
	id            uint32
	streamName    string
	conn          *serverConn
	chunkStreamID uint32
	attachedHandlers []ServerStreamHandler
	handlersLocker sync.Mutex
	bufferLength  uint32
	messageChannel chan *Message
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
func (stream *serverStream) SetStreamName(newName string) {
	stream.streamName = newName 
}

func (stream *serverStream) Handlers() []ServerStreamHandler {
	return stream.attachedHandlers
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
	message.LogDump("closeStream")
	conn := stream.conn.Conn()
	conn.Send(message)
}
         
func (stream *serverStream) StreamMessageReceiver() (chan<- *Message) {
	return stream.messageChannel;
}

func (stream *serverStream) Attach(handler ServerStreamHandler) []ServerStreamHandler {
	stream.handlersLocker.Lock()
	defer stream.handlersLocker.Unlock()

	stream.attachedHandlers = append(stream.attachedHandlers, handler)
	//log.Printf("[stream %d] *** new handler count = %d", stream.ID(), len(stream.attachedHandlers))
	return stream.attachedHandlers  
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

func ReceiveStreamMessage(stream ServerStream, message *Message) bool {
	if (DebugLog) {
		log.Printf("[stream %d][cs %d] server received msg, type = %d(%s)", stream.ID(), stream.ChunkStreamID(), message.Type, message.TypeDisplay())
	}
	if message.Type == VIDEO_TYPE || message.Type == AUDIO_TYPE {
		return receiveNetStreamPayload(stream, message)
	}
	if message.Type == COMMAND_AMF0 || message.Type == COMMAND_AMF3 {
		return parseStreamCommandMessage(stream, message)
	}
	return false
}

func receiveNetStreamPayload(stream ServerStream, message *Message) bool {
	
	handlers := stream.Handlers()
	
	if (len(handlers) == 0) {
		return false
	}
	
	mtype := message.Type
	for _, handler := range handlers {
		switch mtype {
			case AUDIO_TYPE:
				handler.OnAudioData(stream, message)
			case VIDEO_TYPE:
				handler.OnVideoData(stream, message)
		}
		
	}
	
	return true
}

func parseStreamCommandMessage(stream ServerStream, message *Message) bool {
	var err error
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
	
	// command-specific behavior: 
	// parses more parameters from command
	// delegates to handlers
	switch cmd.Name {
	case "play":
		return onPlayCommand(stream, cmd)
	case "publish":
		return onPublishCommand(stream, cmd)
	case "receiveAudio":
		return onReceiveAudioCommand(stream, cmd)
	case "receiveVideo":
		return onReceiveVideoCommand(stream, cmd)
	case "closeStream":
		return onCloseStreamCommand(stream, cmd)
	default:
		log.Printf("serverStream::Received, but unhandled: %+v\n", cmd)
	}
	return false
}

func onPlayCommand(stream ServerStream, cmd *Command) bool {
	var streamName string
	var exists bool
	
	if streamName, exists = cmd.ObjectString(1); !exists {
		log.Printf("play command: cannot extract stream name, failing")
		return true
	}
	
	var start float64
	start, exists = cmd.ObjectNumber(2)
	if (!exists) {
		start = -2
	}

	var duration float64
	duration, exists = cmd.ObjectNumber(3)
	if (!exists) {
		duration = -1
	}

	var flushPrevPlaylist bool
	flushPrevPlaylist, exists = cmd.ObjectBool(4)
	if (!exists) {
		flushPrevPlaylist = false
	}

	log.Printf("[stream %d][cs %d] onPlay: name=%q, start=%f, duration=%f, flush=%t", stream.ID(), stream.ChunkStreamID(),
		streamName, start, duration, flushPrevPlaylist)
	
	stream.SetStreamName(streamName)
	
	// Response
	stream.Conn().Conn().SetChunkSize(4096)
	stream.Conn().Conn().SendUserControlMessage(EVENT_STREAM_BEGIN)
	streamReset(stream)
	streamStart(stream)
	rtmpSampleAccess(stream)

	handlers := stream.Handlers()
	for _, handler := range handlers {
		go func() {
			handler.OnPlayStart(stream, streamName, start, duration, flushPrevPlaylist)
		} ()
	}
	return true
}

func onPublishCommand(stream ServerStream, cmd *Command) bool {

	// extract from command
	publishingName, _ := cmd.ObjectString(1)
	publishingType, _ := cmd.ObjectString(2)

	log.Printf("[stream %d][cs %d] onPublish %q/%q", stream.ID(), stream.ChunkStreamID(), publishingName, publishingType)

	stream.SetStreamName(publishingName)
	
	handlers := stream.Handlers()
	for index, handler := range handlers {
		go func() {
			log.Printf("onPublish handler %d", index)
			handler.OnPublishStart(stream, publishingName, publishingType)
		} ()
	}
	return true
}
func onReceiveAudioCommand(stream ServerStream, cmd *Command) bool {
	log.Printf("[stream %d][cs %d] onReceiveAudio", stream.ID(), stream.ChunkStreamID())
	cmd.LogDump("onReceiveAudioCommand")
	
	// TODO parse command parameter "Bool flag"
	requestingData := true

	handlers := stream.Handlers()
	for _, handler := range handlers {
		go func() {
			handler.OnReceiveAudio(stream, requestingData)
		} ()
	}
	return true
}
func onReceiveVideoCommand(stream ServerStream, cmd *Command) bool {
	log.Printf("[stream %d][cs %d] onReceiveAudio", stream.ID(), stream.ChunkStreamID())
	cmd.LogDump("onReceiveVideoCommand")

	// TODO parse command parameter "Bool flag"
	requestingData := true

	handlers := stream.Handlers()
	for _, handler := range handlers {
		go func() {
			handler.OnReceiveVideo(stream, requestingData)
		} ()
	}
	return true
}
func onCloseStreamCommand(stream ServerStream, cmd *Command) bool {
	log.Printf("[stream %d][cs %d] onCloseStream **empty**", stream.ID(), stream.ChunkStreamID())
	return true
}

func streamReset(stream ServerStream) {
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
		"description": fmt.Sprintf("playing and resetting %s", stream.StreamName()),
		"details":     stream.StreamName(),
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
	message.LogDump("streamReset")
	stream.Conn().Conn().Send(message)
}

func streamStart(stream ServerStream) {
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
		"description": fmt.Sprintf("Started playing %s", stream.StreamName()),
		"details":     stream.StreamName(),
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
	message.LogDump("streamStart")
	stream.Conn().Conn().Send(message)
}

func rtmpSampleAccess(stream ServerStream) {
	message := NewMessage(CS_ID_USER_CONTROL, DATA_AMF0, 0, 0, nil)
	amf.WriteString(message.Buf, "|RtmpSampleAccess")
	amf.WriteBoolean(message.Buf, false)
	amf.WriteBoolean(message.Buf, false)
	message.LogDump("rtmpSampleAccess")
	stream.Conn().Conn().Send(message)
}

// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/berndfo/goamf"
	"log"
	"net"
	"sync"
	"time"
)

const (
	SERVER_CONN_STATUS_CLOSE = uint(0)
	SERVER_CONN_STATUS_CONNECT_OK = uint(1)
	SERVER_CONN_STATUS_CREATE_STREAM_OK = uint(2)
)

// A handler for inbound connection
type ServerAuthHandler interface {
	OnConnectAuth(srvConn ServerConn, connectReq *Command) bool
}

// A handler for inbound connection
type ServerConnHandler interface {
	ConnHandler
	// When connection status changed
	OnStatus(srvConn ServerConn)
	// On stream created
	OnStreamCreated(srvConn ServerConn, stream ServerStream)
	// On stream closed
	OnStreamClosed(srvConn ServerConn, stream ServerStream)
}

type ServerConn interface {
	// Close a connection
	Close()
	// Connection status
	Status() (uint, error)
	// Send a message
	Send(message *Message) error
	// Calls a command or method on Flash Media Server
	// or on an application server running Flash Remoting.
	Call(customParameters ...interface{}) (err error)
	// Get network connect instance
	Conn() Conn
	// Attach handler
	Attach(handler ServerConnHandler)
	// Get connect request
	ConnectRequest() *Command
}

type serverConn struct {
	processCloseOnce sync.Once 
	serverConnLostChan chan<- ServerConn 
	
	connectReq    *Command
	app           string
	handler ServerConnHandler
	authHandler ServerAuthHandler
	conn          Conn
	status        uint
	err           error
	streams       map[uint32]*serverStream
	streamsLocker sync.Mutex
}

func NewServerConn(c net.Conn, br *bufio.Reader, bw *bufio.Writer,
	authHandler ServerAuthHandler, maxChannelNumber int, serverConnLostChan chan<- ServerConn) (ServerConn, error) {
	srvConn := &serverConn{
		authHandler: authHandler,
		status:      SERVER_CONN_STATUS_CLOSE,
		streams:     make(map[uint32]*serverStream),
		serverConnLostChan: serverConnLostChan, 
	}
	srvConn.conn = NewConn(c, br, bw, srvConn, maxChannelNumber)
	return srvConn, nil
}

// Callback when recieved message. Audio & Video data
func (srvConn *serverConn) OnConnMessageReceived(conn Conn, message *Message) {
	stream, found := srvConn.streams[message.MessageStreamID]
	if found {
		stream.StreamMessageReceiver() <- message
	} else {
		// TODO understand and refactor
		// I cannot get my head around this yet. In which cases does it make sense
		// to not handle a message within a stream context?
		// should above delegation to stream be done below within OnConnMessageReceived?
		log.Printf("[%s]*** message handling not done by stream, but server conn", conn.Id())
		srvConn.handler.OnConnMessageReceived(conn, message)
	}
}

// Callback when recieved message.
func (srvConn *serverConn) OnReceivedRtmpCommand(conn Conn, command *Command) {
	//command.LogDump("OnReceivedRtmpCommand")
	switch command.Name {
	case "connect":
		// Connect from client
		srvConn.onConnect(conn, command)
	case "createStream":
		// Create a new stream
		srvConn.onCreateStream(conn, command)
	default:
		log.Printf("[%s] serverConn::ReceivedRtmpCommand unhandled: %s", conn.Id(), command.Dump("recvdCmd"))
	}
}

// Connection closed
func (srvConn *serverConn) OnClosed(conn Conn) {
	srvConn.status = SERVER_CONN_STATUS_CLOSE
	srvConn.handler.OnStatus(srvConn)
	srvConn.Close()
}

// Close a connection
func (srvConn *serverConn) Close() {
	srvConn.processCloseOnce.Do(func () {
		for _, stream := range srvConn.streams {
			stream.Close()
		}
		time.Sleep(time.Second)
		srvConn.status = SERVER_CONN_STATUS_CLOSE
		srvConn.conn.Close()
		srvConn.serverConnLostChan <- srvConn
	})
}

// Send a message
func (srvConn *serverConn) Send(message *Message) error {
	return srvConn.conn.Send(message)
}

// Calls a command or method on Flash Media Server
// or on an application server running Flash Remoting.
func (srvConn *serverConn) Call(customParameters ...interface{}) (err error) {
	return errors.New("Unimplemented")
}

// Get network connect instance
func (srvConn *serverConn) Conn() Conn {
	return srvConn.conn
}

// Connection status
func (srvConn *serverConn) Status() (uint, error) {
	return srvConn.status, srvConn.err
}
func (srvConn *serverConn) Attach(handler ServerConnHandler) {
	srvConn.handler = handler
}

////////////////////////////////
// Local functions

func (srvConn *serverConn) allocStream(stream *serverStream) uint32 {
	srvConn.streamsLocker.Lock()
	defer srvConn.streamsLocker.Unlock()
	
	i := uint32(1)
	for {
		_, found := srvConn.streams[i]
		if !found {
			srvConn.streams[i] = stream
			stream.id = i
			break
		}
		i++
	}
	
	return i
}

func (srvConn *serverConn) releaseStream(streamID uint32) {
	srvConn.streamsLocker.Lock()
	delete(srvConn.streams, streamID)
	srvConn.streamsLocker.Unlock()
}

func (srvConn *serverConn) onConnect(conn Conn, cmd *Command) {
	log.Printf("[%s] serverConn::onConnect: %s", conn.Id(), cmd.Dump(""))
	srvConn.connectReq = cmd
	if cmd.Objects == nil {
		log.Printf("[%s] serverConn::onConnect cmd.Object == nil", conn.Id())
		srvConn.sendConnectErrorResult(cmd)
		return
	}
	if len(cmd.Objects) == 0 {
		log.Printf("[%s] serverConn::onConnect len(cmd.Object) == 0", conn.Id())
		srvConn.sendConnectErrorResult(cmd)
		return
	}
	params, ok := cmd.Objects[0].(amf.Object)
	if !ok {
		log.Printf("[%s] serverConn::onConnect cmd.Object[0] is not an amd object", conn.Id())
		srvConn.sendConnectErrorResult(cmd)
		return
	}

	// Get app
	app, found := params["app"]
	if !found {
		log.Printf("[%s] serverConn::onConnect no app value in cmd.Object[0]", conn.Id())
		srvConn.sendConnectErrorResult(cmd)
		return
	}
	srvConn.app, ok = app.(string)
	if !ok {
		log.Printf("[%s] serverConn::onConnect cmd.Object[0].app is not a string", conn.Id())
		srvConn.sendConnectErrorResult(cmd)
		return
	}

	// Todo: Get version for log
	// Todo: Get other paramters
	// Todo: Auth by logical
	if srvConn.authHandler.OnConnectAuth(srvConn, cmd) {
		//go func() {
			srvConn.conn.SetWindowAcknowledgementSize(32000, 32000)
			srvConn.conn.SetPeerBandwidth(25000, SET_PEER_BANDWIDTH_DYNAMIC)
			srvConn.conn.SetChunkSize(8*DEFAULT_CHUNK_SIZE)
			conn.SendUserControlMessage(EVENT_STREAM_BEGIN)
			log.Printf("[%s] serverConn::onConnect sending success result for app %q", conn.Id(), app)
			srvConn.sendConnectSucceededResult(cmd)
		//}()
	} else {
		srvConn.sendConnectErrorResult(cmd)
		log.Printf("[%s] serverConn::onConnect sending ERROR result for app %q", conn.Id(), app)
	}
}

func (srvConn *serverConn) onCreateStream(conn Conn, cmd *Command) {
	log.Printf("[%s] serverConn::onCreateStream", conn.Id())
	// New inbound stream
	newChunkStream, err := srvConn.conn.CreateMediaChunkStream()
	if err != nil {
		log.Printf("[%s], conn.Id()serverConn::ReceivedCommand() CreateMediaChunkStream err: %s", conn.Id(), err.Error())
		return
	}
	
	msgChan := make(chan *Message, DEFAULT_HIGH_PRIORITY_BUFFER_SIZE)
	stream := serverStream{
		conn:          srvConn,
		chunkStreamID: newChunkStream.ID,
		attachedHandlers: make([]ServerStreamHandler, 0),
		messageChannel: msgChan,
	}
	// message receiver loop for new stream
	go func() {
		var throughput = 0
		printThroughputDuration := 1*time.Second
		printThroughput := time.After(printThroughputDuration)
		for {
			select {
				case message := <-msgChan:
					if message == nil {
						return;
					}
					throughput++
					//message.AbsoluteTimestamp = uint32(time.Now().UnixNano()/(1000*10))
					//log.Printf("[%s] handling stream message, type = %d(%s), at %d", conn.Id(), message.Type, message.TypeDisplay(), message.AbsoluteTimestamp)
					handled := ReceiveStreamMessage(&stream, message)
					if (!handled) {
						log.Printf("[%s] unhandled stream message, type = %d(%s)", conn.Id(), message.Type, message.TypeDisplay())
						message.LogDump("unhandled stream message")
					}
				case <-printThroughput:
					log.Printf("[%s] received msg throughput %d/%s", conn.Id(), throughput, printThroughputDuration)
					throughput = 0
					printThroughput = time.After(printThroughputDuration)
			
				case <-time.After(30*time.Minute):
					log.Printf("[%s] pending stream %d with no message received", conn.Id(), newChunkStream.ID)
			}
		}
	}()
	
	srvConn.allocStream(&stream)
	srvConn.status = SERVER_CONN_STATUS_CREATE_STREAM_OK
	srvConn.handler.OnStatus(srvConn) // TODO ???
	srvConn.handler.OnStreamCreated(srvConn, &stream)
	// Response result
	srvConn.sendCreateStreamSuccessResult(cmd)
}

func (srvConn *serverConn) onCloseStream(stream *serverStream) {
	close(stream.messageChannel)
	srvConn.releaseStream(stream.id)
	srvConn.handler.OnStreamClosed(srvConn, stream)
}

func (srvConn *serverConn) sendConnectSucceededResult(req *Command) {
	obj1 := make(amf.Object)
	obj1["fmsVer"] = fmt.Sprintf("FMS/%s", FMS_VERSION_STRING)
	obj1["capabilities"] = float64(255)
	//obj1["audioCodecs"] = float64(0x400) // AAC, Nelly Moser 16khz, mp3 
	obj1["audioCodecs"] = float64(0xfff) // all audio
	obj1["videoCodecs"] = float64(0x0ff) // all video
	obj2 := make(amf.Object)
	obj2["level"] = "status"
	obj2["code"] = RESULT_CONNECT_OK
	obj2["description"] = RESULT_CONNECT_OK_DESC
	srvConn.sendConnectResult(req, "_result", obj1, obj2)
}

func (srvConn *serverConn) sendConnectErrorResult(req *Command) {
	obj2 := make(amf.Object)
	obj2["level"] = "status"
	obj2["code"] = RESULT_CONNECT_REJECTED
	obj2["description"] = RESULT_CONNECT_REJECTED_DESC
	srvConn.sendConnectResult(req, "_error", nil, obj2)
}

func (srvConn *serverConn) sendConnectResult(req *Command, name string, obj1, obj2 interface{}) (err error) {
	// Create createStream command
	cmd := &Command{
		IsFlex:        false,
		Name:          name,
		TransactionID: req.TransactionID,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = obj1
	cmd.Objects[1] = obj2
	buf := new(bytes.Buffer)
	err = cmd.Write(buf)
	//log.Printf("cmd buffer dump: %s", hex.Dump(buf.Bytes()))
	CheckError(err, "serverConn::sendConnectResult() Create command")
	cmd.LogDump("sendConnectResult cmd:")

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.LogDump("sendConnectResult msg:")
	return srvConn.conn.Send(message)

}

func (srvConn *serverConn) sendCreateStreamSuccessResult(req *Command) (err error) {
	// Create createStream command
	cmd := &Command{
		IsFlex:        false,
		Name:          "_result",
		TransactionID: req.TransactionID,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = int32(1)
	buf := new(bytes.Buffer)
	err = cmd.Write(buf)
	CheckError(err, "serverConn::sendCreateStreamSuccessResult() Create command")

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.LogDump("sendCreateStreamSuccessResult")
	return srvConn.conn.Send(message)

}

func (srvConn *serverConn) ConnectRequest() *Command {
	return srvConn.connectReq
}

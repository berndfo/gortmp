// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/berndfo/goamf"
	"log"
	"net"
	"time"
)

const (
	CLIENT_CONN_STATUS_CLOSE = uint(0)
	CLIENT_CONN_STATUS_HANDSHAKE_OK = uint(1)
	CLIENT_CONN_STATUS_CONNECT = uint(2)
	CLIENT_CONN_STATUS_CONNECT_OK = uint(3)
	CLIENT_CONN_STATUS_CREATE_STREAM = uint(4)
	CLIENT_CONN_STATUS_CREATE_STREAM_OK = uint(5)
)

func StatusDisplay(status uint) string {
	switch status {
		case 0: return "close"
		case 1: return "handshake-ok"
		case 2: return "connect"
		case 3: return "connect-ok"
		case 4: return "create-stream"
		case 5: return "create-stream-ok"
		default: return "unkown-status"
	}
} 

// A handler for outbound client connection
type ClientConnHandler interface {
	ConnHandler
	// When connection status changed
	OnStatus(obConn ClientConn)
	// On stream created
	OnStreamCreated(obConn ClientConn, stream ClientStream)
}

type ClientConn interface {
	// Connect an appliction on FMS after handshake.
	Connect(extendedParameters ...interface{}) (err error)
	// Create a stream
	CreateStream() (err error)
	// Close a connection
	Close()
	// URL to connect
	URL() string
	// Connection status
	Status() (uint, error)
	// Send a message
	Send(message *Message) error
	// Calls a command or method on Flash Media Server
	// or on an application server running Flash Remoting.
	Call(name string, customParameters ...interface{}) (err error)
	// Get network connect instance
	Conn() Conn
}

// High-level interface
//
// A RTMP connection(based on TCP) to RTMP server(FMS or crtmpserver).
// In one connection, we can create many chunk streams.
type clientConn struct {
	url          string
	rtmpURL      RtmpURL
	status       uint
	err          error
	handler ClientConnHandler
	conn         Conn
	transactions map[uint32]string
	streams      map[uint32]ClientStream
}

// Connect to FMS server, and finish handshake process
func Dial(url string, handler ClientConnHandler, maxChannelNumber int) (ClientConn, error) {
	rtmpURL, err := ParseURL(url)
	if err != nil {
		return nil, err
	}
	var c net.Conn
	switch rtmpURL.protocol {
	case "rtmp":
		c, err = net.Dial("tcp", fmt.Sprintf("%s:%d", rtmpURL.host, rtmpURL.port))
	case "rtmps":
		c, err = tls.Dial("tcp", fmt.Sprintf("%s:%d", rtmpURL.host, rtmpURL.port), &tls.Config{InsecureSkipVerify: true})
	default:
		err = errors.New(fmt.Sprintf("Unsupport protocol %s", rtmpURL.protocol))
	}
	if err != nil {
		return nil, err
	}

	ipConn, ok := c.(*net.TCPConn)
	if ok {
		ipConn.SetWriteBuffer(128 * 1024)
	}
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	timeout := time.Duration(10*time.Second)
	err = Handshake(c, br, bw, timeout)
	//err = HandshakeSample(c, br, bw, timeout)
	if err == nil {
		log.Printf("Handshake OK, url: %s", url)

		client := &clientConn{
			url:          url,
			rtmpURL:      rtmpURL,
			handler:      handler,
			status:       CLIENT_CONN_STATUS_HANDSHAKE_OK,
			transactions: make(map[uint32]string),
			streams:      make(map[uint32]ClientStream),
		}
		client.conn = NewConn(c, br, bw, client, maxChannelNumber)
		return client, nil
	}

	return nil, err
}

// Connect to FMS server, and finish handshake process
func NewClientConn(c net.Conn, url string, handler ClientConnHandler, maxChannelNumber int) (ClientConn, error) {
	rtmpURL, err := ParseURL(url)
	if err != nil {
		return nil, err
	}
	if rtmpURL.protocol != "rtmp" {
		return nil, errors.New(fmt.Sprintf("Unsupport protocol %s", rtmpURL.protocol))
	}
	/*
		ipConn, ok := c.(*net.TCPConn)
		if ok {
			ipConn.SetWriteBuffer(128 * 1024)
		}
	*/
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	client := &clientConn{
		url:          url,
		rtmpURL:      rtmpURL,
		handler:      handler,
		status:       CLIENT_CONN_STATUS_HANDSHAKE_OK,
		transactions: make(map[uint32]string),
		streams:      make(map[uint32]ClientStream),
	}
	client.conn = NewConn(c, br, bw, client, maxChannelNumber)
	return client, nil
}

// Connect an appliction on FMS after handshake.
func (client *clientConn) Connect(extendedParameters ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			if client.err == nil {
				client.err = err
			}
		}
	}()
	// Create connect command
	buf := new(bytes.Buffer)
	// Command name
	_, err = amf.WriteString(buf, "connect")
	CheckError(err, "Connect() Write name: connect")
	transactionID := client.conn.NewTransactionID()
	client.transactions[transactionID] = "connect"
	_, err = amf.WriteDouble(buf, float64(transactionID))
	CheckError(err, "Connect() Write transaction ID")
	_, err = amf.WriteObjectMarker(buf)
	CheckError(err, "Connect() Write object marker")

	_, err = amf.WriteObjectName(buf, "app")
	CheckError(err, "Connect() Write app name")
	_, err = amf.WriteString(buf, client.rtmpURL.App())
	CheckError(err, "Connect() Write app value")

	_, err = amf.WriteObjectName(buf, "flashVer")
	CheckError(err, "Connect() Write flashver name")
	_, err = amf.WriteString(buf, FLASH_PLAYER_VERSION_STRING)
	CheckError(err, "Connect() Write flashver value")

	//	_, err = amf.WriteObjectName(buf, "swfUrl")
	//	CheckError(err, "Connect() Write swfUrl name")
	//	_, err = amf.WriteString(buf, SWF_URL_STRING)
	//	CheckError(err, "Connect() Write swfUrl value")

	_, err = amf.WriteObjectName(buf, "tcUrl")
	CheckError(err, "Connect() Write tcUrl name")
	_, err = amf.WriteString(buf, client.url)
	CheckError(err, "Connect() Write tcUrl value")

	_, err = amf.WriteObjectName(buf, "fpad")
	CheckError(err, "Connect() Write fpad name")
	_, err = amf.WriteBoolean(buf, false)
	CheckError(err, "Connect() Write fpad value")

	_, err = amf.WriteObjectName(buf, "capabilities")
	CheckError(err, "Connect() Write capabilities name")
	_, err = amf.WriteDouble(buf, DEFAULT_CAPABILITIES)
	CheckError(err, "Connect() Write capabilities value")

	_, err = amf.WriteObjectName(buf, "audioCodecs")
	CheckError(err, "Connect() Write audioCodecs name")
	_, err = amf.WriteDouble(buf, DEFAULT_AUDIO_CODECS)
	CheckError(err, "Connect() Write audioCodecs value")

	_, err = amf.WriteObjectName(buf, "videoCodecs")
	CheckError(err, "Connect() Write videoCodecs name")
	_, err = amf.WriteDouble(buf, DEFAULT_VIDEO_CODECS)
	CheckError(err, "Connect() Write videoCodecs value")

	_, err = amf.WriteObjectName(buf, "videoFunction")
	CheckError(err, "Connect() Write videoFunction name")
	_, err = amf.WriteDouble(buf, float64(1))
	CheckError(err, "Connect() Write videoFunction value")

	//	_, err = amf.WriteObjectName(buf, "pageUrl")
	//	CheckError(err, "Connect() Write pageUrl name")
	//	_, err = amf.WriteString(buf, PAGE_URL_STRING)
	//	CheckError(err, "Connect() Write pageUrl value")

	//_, err = amf.WriteObjectName(buf, "objectEncoding")
	//CheckError(err, "Connect() Write objectEncoding name")
	//_, err = amf.WriteDouble(buf, float64(amf.AMF0))
	//CheckError(err, "Connect() Write objectEncoding value")

	_, err = amf.WriteObjectEndMarker(buf)
	CheckError(err, "Connect() Write ObjectEndMarker")

	// extended parameters
	for _, param := range extendedParameters {
		_, err = amf.WriteValue(buf, param)
		CheckError(err, "Connect() Write extended parameters")
	}
	connectMessage := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	connectMessage.LogDump("connect")
	client.status = CLIENT_CONN_STATUS_CONNECT
	return client.conn.Send(connectMessage)
}

// Close a connection
func (client *clientConn) Close() {
	for _, stream := range client.streams {
		stream.Close()
	}
	client.status = CLIENT_CONN_STATUS_CLOSE
	go func() {
		time.Sleep(time.Second)
		client.conn.Close()
	}()
}

// URL to connect
func (client *clientConn) URL() string {
	return client.url
}

// Connection status
func (client *clientConn) Status() (uint, error) {
	return client.status, client.err
}

// Callback when recieved message. Audio & Video data
func (client *clientConn) 	OnConnMessageReceived(conn Conn, message *Message) {
	stream, found := client.streams[message.MessageStreamID]
	if found {
		if !stream.Received(message) {
			client.handler.OnConnMessageReceived(conn, message)
		}
	} else {
		client.handler.OnConnMessageReceived(conn, message)
	}
}

// Callback when recieved message.
func (client *clientConn) OnReceivedRtmpCommand(conn Conn, command *Command) {
	command.LogDump("")
	switch command.Name {
	case "_result":
		transaction, found := client.transactions[command.TransactionID]
		if found {
			switch transaction {
			case "connect":
				if command.Objects != nil && len(command.Objects) >= 2 {
					information, ok := command.Objects[1].(amf.Object)
					if ok {
						code, ok := information["code"]
						if ok && code == RESULT_CONNECT_OK {
							// Connect OK
							//time.Sleep(time.Duration(200) * time.Millisecond)
							client.conn.SetWindowAcknowledgementSize()
							client.status = CLIENT_CONN_STATUS_CONNECT_OK
							client.handler.OnStatus(client)
							client.status = CLIENT_CONN_STATUS_CREATE_STREAM
							client.CreateStream()
						}
					}
				}
			case "createStream":
				if command.Objects != nil && len(command.Objects) >= 2 {
					streamID, ok := command.Objects[1].(float64)
					if ok {
						newChunkStream, err := client.conn.CreateMediaChunkStream()
						if err != nil {
							log.Printf(
								"clientConn::ReceivedRtmpCommand() CreateMediaChunkStream err:", err)
							return
						}
						stream := &clientStream{
							id:            uint32(streamID),
							conn:          client,
							chunkStreamID: newChunkStream.ID,
						}
						client.streams[stream.ID()] = stream
						client.status = CLIENT_CONN_STATUS_CREATE_STREAM_OK
						client.handler.OnStatus(client)
						client.handler.OnStreamCreated(client, stream)
					}
				}
			}
			delete(client.transactions, command.TransactionID)
		}
	case "_error":
		transaction, found := client.transactions[command.TransactionID]
		if found {
			log.Printf(
				"Command(%d) %s error\n", command.TransactionID, transaction)
		} else {
			log.Printf(
				"Command(%d) not been found\n", command.TransactionID)
		}
	case "onBWCheck":
	}
	client.handler.OnReceivedRtmpCommand(client.conn, command)
}

// Connection closed
func (client *clientConn) OnClosed(conn Conn) {
	client.status = CLIENT_CONN_STATUS_CLOSE
	client.handler.OnStatus(client)
	client.handler.OnClosed(conn)
}

// Create a stream
func (client *clientConn) CreateStream() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			if client.err == nil {
				client.err = err
			}
		}
	}()
	// Create createStream command
	transactionID := client.conn.NewTransactionID()
	cmd := &Command{
		IsFlex:        false,
		Name:          "createStream",
		TransactionID: transactionID,
		Objects:       make([]interface{}, 1),
	}
	cmd.Objects[0] = nil
	buf := new(bytes.Buffer)
	err = cmd.Write(buf)
	CheckError(err, "createStream() Create command")
	client.transactions[transactionID] = "createStream"

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.LogDump("createStream")
	return client.conn.Send(message)
}

// Send a message
func (client *clientConn) Send(message *Message) error {
	return client.conn.Send(message)
}

// Calls a command or method on Flash Media Server
// or on an application server running Flash Remoting.
func (client *clientConn) Call(name string, customParameters ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			if client.err == nil {
				client.err = err
			}
		}
	}()
	// Create command
	transactionID := client.conn.NewTransactionID()
	cmd := &Command{
		IsFlex:        false,
		Name:          name,
		TransactionID: transactionID,
		Objects:       make([]interface{}, 1+len(customParameters)),
	}
	cmd.Objects[0] = nil
	for index, param := range customParameters {
		cmd.Objects[index+1] = param
	}
	buf := new(bytes.Buffer)
	err = cmd.Write(buf)
	CheckError(err, "Call() Create command")
	client.transactions[transactionID] = name

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.LogDump(name)
	return client.conn.Send(message)

}

// Get network connect instance
func (client *clientConn) Conn() Conn {
	return client.conn
}

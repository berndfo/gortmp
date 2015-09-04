// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/berndfo/goamf"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
)

var DebugLog bool = false
// Conn
//
// Common connection functions
type Conn interface {
	Id() string
	Close()
	Send(message *Message) error
	CreateChunkStream(ID uint32) (*ClientChunkStream, error)
	CloseChunkStream(ID uint32)
	NewTransactionID() uint32
	CreateMediaChunkStream() (*ClientChunkStream, error)
	CloseMediaChunkStream(id uint32)
	SetStreamBufferSize(streamId uint32, size uint32)
	ClientChunkStream(id uint32) (chunkStream *ClientChunkStream, found bool)
	ServerChunkStream(id uint32) (chunkStream *ServerChunkStream, found bool)
	SetWindowAcknowledgementSize(inWindowSize uint32, outWindowSize uint32)
	SetPeerBandwidth(peerBandwidth uint32, limitType byte)
	SetChunkSize(chunkSize uint32)
	SendUserControlMessage(eventId uint16)
}

// Connection handler
type ConnHandler interface {
	// Received message
	OnConnMessageReceived(conn Conn, message *Message)
	// Received command
	OnReceivedRtmpCommand(conn Conn, command *Command)
	// Connection closed
	OnClosed(conn Conn)
}

// conn
//
// To maintain all chunk streams in one network connection.
// 
// implements Conn
type conn struct {
	id string
	established time.Time
	
	// Chunk streams
	outChunkStreams map[uint32]*ClientChunkStream
	inChunkStreams  map[uint32]*ServerChunkStream

	// High-priority send message buffer.
	// Protocol control messages are sent with highest priority.
	messageQueue chan *Message
	messageOffset int

	// Chunk size
	inChunkSize      uint32
	outChunkSize     uint32
	outChunkSizeTemp uint32

	// Bytes counter(For window ack)
	inBytes  uint32
	outBytes uint32
	
	// conn statistics
	inMessages uint64
	inMessagesPerType map[uint8]uint64
	outMessages uint64
	outMessagesPerType map[uint8]uint64
	inChunks uint64
	allChunkStreamIds []uint32
	logStatisticsEvery time.Duration

	// Previous window acknowledgement inbytes
	inBytesPreWindow uint32

	// Window size
	inWindowSize  uint32
	outWindowSize uint32

	// Bandwidth
	inBandwidth  uint32
	outBandwidth uint32

	// Bandwidth Limit
	inBandwidthLimit  uint8
	outBandwidthLimit uint8

	// Media chunk stream ID
	mediaChunkStreamIDAllocator       []bool
	mediaChunkStreamIDAllocatorLocker sync.Mutex

	// Closed
	closed bool

	// Handler
	handler ConnHandler

	// Network connection
	c  net.Conn
	br *bufio.Reader
	bw *bufio.Writer

	// Last transaction ID
	lastTransactionID uint32

	// Error
	err error
}

// Create new connection
func NewConn(c net.Conn, br *bufio.Reader, bw *bufio.Writer, handler ConnHandler, maxChannelNumber int) Conn {
	conn := &conn{
		id: c.RemoteAddr().String(),
		established: time.Now(),
		c:                           c,
		br:                          br,
		bw:                          bw,
		outChunkStreams:             make(map[uint32]*ClientChunkStream),
		inChunkStreams:              make(map[uint32]*ServerChunkStream),
		messageQueue:                make(chan *Message, DEFAULT_HIGH_PRIORITY_BUFFER_SIZE),
		inChunkSize:                 DEFAULT_CHUNK_SIZE,
		outChunkSize:                DEFAULT_CHUNK_SIZE,
		inWindowSize:                DEFAULT_WINDOW_SIZE,
		outWindowSize:               DEFAULT_WINDOW_SIZE,
		inMessagesPerType:           make(map[uint8]uint64),
		outMessagesPerType:          make(map[uint8]uint64),
	    allChunkStreamIds:           make([]uint32, 0),
		logStatisticsEvery:          time.Duration(5*time.Second),
		inBandwidth:                 DEFAULT_WINDOW_SIZE,
		outBandwidth:                DEFAULT_WINDOW_SIZE,
		inBandwidthLimit:            BINDWIDTH_LIMIT_DYNAMIC,
		outBandwidthLimit:           BINDWIDTH_LIMIT_DYNAMIC,
		handler:                     handler,
		mediaChunkStreamIDAllocator: make([]bool, maxChannelNumber),
	}
	// Create "Protocol control chunk stream"
	conn.outChunkStreams[CS_ID_PROTOCOL_CONTROL] = NewClientChunkStream(CS_ID_PROTOCOL_CONTROL)
	// Create "Command message chunk stream"
	conn.outChunkStreams[CS_ID_COMMAND] = NewClientChunkStream(CS_ID_COMMAND)
	// Create "User control chunk stream"
	conn.outChunkStreams[CS_ID_USER_CONTROL] = NewClientChunkStream(CS_ID_USER_CONTROL)

	go func() {
		for {
			connectionUptime := time.Since(conn.established).String()
			log.Printf("[%s] write statistics: send msg count %6d, total bytes out %9d, msg type counter = %v, duration %s, chunk streams = %v", conn.Id(), conn.outMessages, conn.outBytes, conn.outMessagesPerType, connectionUptime, conn.allChunkStreamIds)
			log.Printf("[%s] read  statistics: rcvd msg count %6d, total bytes  in %9d, msg type counter = %v, duration %s, chunk streams = %v, chunk count %d", conn.Id(), conn.inMessages, conn.inBytes, conn.inMessagesPerType, connectionUptime, conn.allChunkStreamIds, conn.inChunks)
			if conn.closed {
				log.Printf("[%s] connection closed", conn.Id())
				return
			}
			<-time.After(conn.logStatisticsEvery)
		}
	}()

	go conn.sendLoop()
	go conn.readLoop()
	return conn
}

func (conn *conn) Id() string {
	return conn.id
}

// Send high priority message in continuous chunks
func (conn *conn) sendMessage(message *Message) {
	chunkStream, found := conn.outChunkStreams[message.ChunkStreamID]
	if !found {
		log.Printf("Can not found chunk strem id %d", message.ChunkStreamID)
		// Error
		return
	}

	header := chunkStream.NewClientHeader(message)
	bytesWrittenInt, err := header.Write(conn.bw)
	if err != nil {
		conn.error(err, "sendMessage write header")
		return
	}
	conn.outBytes += uint32(bytesWrittenInt)

	var bytesWritten int64
	if header.MessageLength > conn.outChunkSize {
		//		chunkStream.lastHeader = nil
		// Split into some chunk
		bytesWritten, err = CopyNToNetwork(conn.bw, message.Buf, int64(conn.outChunkSize), message.ChunkStreamID)
		if err != nil {
			conn.error(err, "sendMessage copy buffer")
			return
		}
		conn.outBytes += uint32(bytesWritten)
		remain := header.MessageLength - conn.outChunkSize

		// Type 3 chunk
		for {
			err = conn.bw.WriteByte(byte(0xc0 | byte(header.ChunkStreamID)))
			if err != nil {
				conn.error(err, "sendMessage Type 3 chunk header")
				return
			}
			conn.outBytes += uint32(1)
			if remain > conn.outChunkSize {
				bytesWritten, err = CopyNToNetwork(conn.bw, message.Buf, int64(conn.outChunkSize), header.ChunkStreamID)
				if err != nil {
					conn.error(err, "sendMessage copy split buffer 1")
					return
				}
				conn.outBytes += uint32(bytesWritten)
				remain -= conn.outChunkSize
			} else {
				bytesWritten, err = CopyNToNetwork(conn.bw, message.Buf, int64(remain), header.ChunkStreamID)
				if err != nil {
					conn.error(err, "sendMessage copy split buffer 2")
					return
				}
				conn.outBytes += uint32(bytesWritten)
				break
			}
		}
	} else {
		bytesWritten, err = CopyNToNetwork(conn.bw, message.Buf, int64(header.MessageLength), header.ChunkStreamID)
		if err != nil {
			conn.error(err, "sendMessage copy buffer")
			return
		}
		conn.outBytes += uint32(bytesWritten)
	}
	err = FlushToNetwork(conn.bw, message.ChunkStreamID)
	if err != nil {
		conn.error(err, "sendMessage Flush 3")
		return
	}
	if message.ChunkStreamID == CS_ID_PROTOCOL_CONTROL &&
		message.Type == SET_CHUNK_SIZE &&
		conn.outChunkSizeTemp != 0 {
		// Set chunk size
		conn.outChunkSize = conn.outChunkSizeTemp
		conn.outChunkSizeTemp = 0
	}
}

// send loop
func (conn *conn) sendLoop() {
	defer func() {
		if r := recover(); r != nil {
			if conn.err == nil {
				conn.err = r.(error)
			}
		}
		conn.Close()
	}()
	
	for !conn.closed {
		select {
		case message := <-conn.messageQueue:
			if (message == nil) {
				break
			}
			conn.outMessages++
			value, _ := conn.outMessagesPerType[message.Type]
			conn.outMessagesPerType[message.Type] = value + 1
		
			conn.sendMessage(message)
		case <-time.After(time.Second):
			// Check close
		}
	}
}

// read loop
func (conn *conn) readLoop() {

	defer func() {

		if r := recover(); r != nil {
			if conn.err == nil {
				conn.err = r.(error)
				log.Printf("readLoop panic: %s", conn.err.Error())
			}
		}
		conn.handler.OnClosed(conn)
		conn.Close()
	}()
	
	var found bool
	var chunkstream *ServerChunkStream
	var remain uint32
	for !conn.closed {
		// Read basic header
		readBytesCountBasic, fmt, csid, err := ReadBasicHeader(conn.br)
		readBytesCount := readBytesCountBasic
		CheckError(err, "ReadBasicHeader")
		conn.inBytes += uint32(readBytesCount)
		// Get chunk stream
		chunkstream, found = conn.inChunkStreams[csid]
		if !found || chunkstream == nil {
			log.Printf("[%s][cs*%d] create new chunk stream for unkown cs id: %d, fmt: %d\n", conn.Id(), csid, csid, fmt)
			conn.allChunkStreamIds = append(conn.allChunkStreamIds, csid)
			chunkstream = NewServerChunkStream(csid)
			conn.inChunkStreams[csid] = chunkstream
		}
		// Read header
		header := &Header{}
		var readBytesCountMsgHeader int
		readBytesCountMsgHeader, err = header.ReadMessageHeader(conn.br, fmt, csid, chunkstream.lastHeader)
		readBytesCount += readBytesCountMsgHeader
		CheckError(err, "ReadMessageHeader")
		if !found {
			log.Printf("[%s][cs %d] full header for new chunk stream cs id: %d, fmt: %d, header: %+v\n", conn.Id(), csid, csid, fmt, header)
		}
		conn.inBytes += uint32(readBytesCount)
		conn.inChunks += 1
		var absoluteTimestamp uint32
		var message *Message
		switch fmt {
		case HEADER_FMT_FULL:
			chunkstream.lastHeader = header
			absoluteTimestamp = header.Timestamp
		case HEADER_FMT_SAME_STREAM:
			// A new message with same stream ID
			if chunkstream.lastHeader == nil {
				log.Printf("A new message with fmt: %d, csi: %d\n", fmt, csid)
				header.Dump("err")
			} else {
				header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
			}
			chunkstream.lastHeader = header
			absoluteTimestamp = chunkstream.lastInAbsoluteTimestamp + header.Timestamp
		case HEADER_FMT_SAME_LENGTH_AND_STREAM:
			// A new message with same stream ID, message length and message type
			if chunkstream.lastHeader == nil {
				log.Printf("A new message with fmt: %d, csi: %d\n", fmt, csid)
				header.Dump("err")
			}
			header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
			header.MessageLength = chunkstream.lastHeader.MessageLength
			header.MessageTypeID = chunkstream.lastHeader.MessageTypeID
			chunkstream.lastHeader = header
			absoluteTimestamp = chunkstream.lastInAbsoluteTimestamp + header.Timestamp
		case HEADER_FMT_CONTINUATION:
			if chunkstream.receivedMessage != nil {
				// Continuation the previous unfinished message
				message = chunkstream.receivedMessage
			} else {
				//TODO BF: shouldn't we better do sth lk this here?:
				// panic("FMT continuation, but message is nil")
			}
			if chunkstream.lastHeader == nil {
				log.Printf("A new message with fmt: %d, csi: %d\n", fmt, csid)
				header.Dump("err")
			} else {
				header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
				header.MessageLength = chunkstream.lastHeader.MessageLength
				header.MessageTypeID = chunkstream.lastHeader.MessageTypeID
				header.Timestamp = chunkstream.lastHeader.Timestamp
			}
			chunkstream.lastHeader = header
			absoluteTimestamp = chunkstream.lastInAbsoluteTimestamp
		}
		if message == nil {
			// New message
			message = &Message{
				ChunkStreamID:     csid,
				Type:              header.MessageTypeID,
				Timestamp:         header.RealTimestamp(),
				Size:              header.MessageLength,
				MessageStreamID:          header.MessageStreamID,
				Buf:               new(bytes.Buffer),
				IsInbound:         true,
				AbsoluteTimestamp: absoluteTimestamp,
			}
		}
		chunkstream.lastInAbsoluteTimestamp = absoluteTimestamp
		// Read data
		remain = message.Remain()
		var n64 int64
		if remain <= conn.inChunkSize {
			// One chunk message
			for {
				// n64, err = CopyNFromNetwork(message.Buf, conn.br, int64(remain))
				n64, err = io.CopyN(message.Buf, conn.br, int64(remain))
				if err == nil {
					conn.inBytes += uint32(n64)
					if remain <= uint32(n64) {
						break
					} else {
						remain -= uint32(n64)
						log.Printf("Message continue copy remain: %d\n", remain)
						continue
					}
				}
				netErr, ok := err.(net.Error)
				if !ok || !netErr.Temporary() {
					CheckError(err, "Read data 1")
				}
				log.Printf("Message copy blocked!\n")
			}
			// Finished message
			conn.inMessages++
			
			value, _ := conn.inMessagesPerType[message.Type]
			conn.inMessagesPerType[message.Type] = value + 1 
			
			conn.received(message)
			chunkstream.receivedMessage = nil
		} else {
			// Unfinished
			if (DebugLog) {
				log.Printf("Unfinished message(remain: %d, chunksize: %d)\n", remain, conn.inChunkSize)
			}

			remain = conn.inChunkSize
			for {
				// n64, err = CopyNFromNetwork(message.Buf, conn.br, int64(remain))
				n64, err = io.CopyN(message.Buf, conn.br, int64(remain))
				if err == nil {
					conn.inBytes += uint32(n64)
					if remain <= uint32(n64) {
						break
					} else {
						remain -= uint32(n64)
						log.Printf("Unfinish message continue copy remain: %d\n", remain)
						continue
					}
				}
				netErr, ok := err.(net.Error)
				if !ok || !netErr.Temporary() {
					CheckError(err, "Read data 2")
				}
				log.Printf("Unfinish message copy blocked!\n")
			}
			chunkstream.receivedMessage = message
		}

		// Check window
		if conn.inBytes > (conn.inBytesPreWindow + conn.inWindowSize) {
			// Send window acknowledgement
			ackmessage := NewMessage(CS_ID_PROTOCOL_CONTROL, ACKNOWLEDGEMENT, MS_ID_CONTROL_STREAM, absoluteTimestamp+1, nil)
			err = binary.Write(ackmessage.Buf, binary.BigEndian, conn.inBytes)
			CheckError(err, "ACK Message write data")
			conn.inBytesPreWindow = conn.inBytes
			conn.Send(ackmessage)
		}
	}
}

func (conn *conn) error(err error, desc string) {
	log.Printf("[%s] Conn %s err: %s\n", conn.Id(), desc, err.Error())
	if conn.err == nil {
		conn.err = err
	}
	conn.Close()
}

func (conn *conn) Close() {
	conn.closed = true
	conn.c.Close()
}

// Send a message by channel
func (conn *conn) Send(message *Message) error {
	conn.messageQueue <- message
	return nil
}

func (conn *conn) CreateChunkStream(id uint32) (*ClientChunkStream, error) {
	chunkStream, found := conn.outChunkStreams[id]
	if found {
		return nil, errors.New("Chunk stream existed")
	}
	chunkStream = NewClientChunkStream(id)
	conn.outChunkStreams[id] = chunkStream
	return chunkStream, nil
}

func (conn *conn) CloseChunkStream(id uint32) {
	delete(conn.outChunkStreams, id)
}

func (conn *conn) CreateMediaChunkStream() (*ClientChunkStream, error) {
	var newChunkStreamID uint32
	conn.mediaChunkStreamIDAllocatorLocker.Lock()
	for index, occupited := range conn.mediaChunkStreamIDAllocator {
		if !occupited {
			newChunkStreamID = uint32((index+1)*6 + 2)
			log.Printf("index: %d, newChunkStreamID: %d\n", index, newChunkStreamID)
			break
		}
	}
	conn.mediaChunkStreamIDAllocatorLocker.Unlock()
	if newChunkStreamID == 0 {
		return nil, errors.New("No more chunk stream ID to allocate")
	}
	chunkSteam, err := conn.CreateChunkStream(newChunkStreamID)
	if err != nil {
		conn.CloseMediaChunkStream(newChunkStreamID)
		return nil, err
	}
	return chunkSteam, nil
}

func (conn *conn) ClientChunkStream(id uint32) (chunkStream *ClientChunkStream, found bool) {
	chunkStream, found = conn.outChunkStreams[id]
	return
}

func (conn *conn) ServerChunkStream(id uint32) (chunkStream *ServerChunkStream, found bool) {
	chunkStream, found = conn.inChunkStreams[id]
	return
}

func (conn *conn) CloseMediaChunkStream(id uint32) {
	conn.mediaChunkStreamIDAllocatorLocker.Lock()
	conn.mediaChunkStreamIDAllocator[id] = false
	conn.mediaChunkStreamIDAllocatorLocker.Unlock()
	conn.CloseChunkStream(id)
}

func (conn *conn) NewTransactionID() uint32 {
	return atomic.AddUint32(&conn.lastTransactionID, 1)
}

func (conn *conn) received(message *Message) {
	if (DebugLog) {
		message.LogDump(fmt.Sprintf("[%s]", conn.Id()))
	}
	tmpBuf := make([]byte, 4)
	var err error
	var subType byte
	var dataSize uint32
	var timestamp uint32
	var timestampExt byte
	if message.Type == AGGREGATE_MESSAGE_TYPE {
		// Byte stream order
		// Sub message type 1 byte
		// Data size 3 bytes, big endian
		// Timestamp 3 bytes
		// Timestamp extend 1 byte,  result = (result >>> 8) | ((result & 0x000000ff) << 24);
		// 3 bytes ignored
		// Data
		// Previous tag size 4 bytes
		var firstAggregateTimestamp uint32
		for message.Buf.Len() > 0 {
			// Sub type
			subType, err = message.Buf.ReadByte()
			if err != nil {
				log.Printf("[%s] received AGGREGATE_MESSAGE_TYPE read sub type err: %s", conn.Id(), err.Error())
				return
			}

			// data size
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				log.Printf("[%s] received AGGREGATE_MESSAGE_TYPE read data size err: %s", conn.Id(), err)
				return
			}
			dataSize = binary.BigEndian.Uint32(tmpBuf)

			// Timestamp
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				log.Printf("[%s] received AGGREGATE_MESSAGE_TYPE read timestamp err: %s", conn.Id(), err)
				return
			}
			timestamp = binary.BigEndian.Uint32(tmpBuf)

			// Timestamp extend
			timestampExt, err = message.Buf.ReadByte()
			if err != nil {
				log.Printf("[%s] received AGGREGATE_MESSAGE_TYPE read timestamp extend err: %s", conn.Id(), err)
				return
			}
			timestamp |= (uint32(timestampExt) << 24)
			if firstAggregateTimestamp == 0 {
				firstAggregateTimestamp = timestamp
			}

			// Ignore 3 bytes
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				log.Printf("[%s] received AGGREGATE_MESSAGE_TYPE read ignore bytes err: %s", conn.Id(), err)
				return
			}

			subMessage := NewMessage(message.ChunkStreamID, subType, message.MessageStreamID, 0, nil)
			subMessage.Timestamp = 0
			subMessage.IsInbound = true
			subMessage.Size = dataSize
			subMessage.AbsoluteTimestamp = message.AbsoluteTimestamp
			// Data
			_, err = io.CopyN(subMessage.Buf, message.Buf, int64(dataSize))
			if err != nil {
				log.Printf("[%s] received AGGREGATE_MESSAGE_TYPE copy data err: %s", conn.Id(), err)
				return
			}

			// Recursion
			conn.received(subMessage)

			// Previous tag size
			if message.Buf.Len() >= 4 {
				_, err = io.ReadAtLeast(message.Buf, tmpBuf, 4)
				if err != nil {
					log.Printf("[%s] received AGGREGATE_MESSAGE_TYPE read previous tag size err: %s", conn.Id(), err)
					return
				}
				tmpBuf[0] = 0
			} else {
				log.Printf("[%s] received AGGREGATE_MESSAGE_TYPE miss previous tag size", conn.Id())
				break
			}
		}
	} else {
		switch message.ChunkStreamID {
		case CS_ID_PROTOCOL_CONTROL:
			switch message.Type {
			case SET_CHUNK_SIZE:
				conn.invokeSetChunkSize(message)
			case ABORT_MESSAGE:
				conn.invokeAbortMessage(message)
			case ACKNOWLEDGEMENT:
				conn.invokeAcknowledgement(message)
			case USER_CONTROL_MESSAGE:
				conn.invokeUserControlMessage(message)
			case WINDOW_ACKNOWLEDGEMENT_SIZE:
				conn.invokeWindowAcknowledgementSize(message)
			case SET_PEER_BANDWIDTH:
				conn.invokeSetPeerBandwidth(message)
			default:
				log.Printf("Unkown message type %d in Protocol control chunk stream!\n", message.Type)
			}
		case CS_ID_COMMAND:
			if message.MessageStreamID == 0 {
				cmd := &Command{}
				var err error
				var transactionID float64
				var object interface{}
				switch message.Type {
				case COMMAND_AMF3:
					cmd.IsFlex = true
					_, err = message.Buf.ReadByte()
					if err != nil {
						log.Printf("[%s]Read first in flex commad err: %s", conn.Id(), err)
						return
					}
					fallthrough
				case COMMAND_AMF0:
					cmd.Name, err = amf.ReadString(message.Buf)
					if err != nil {
						log.Printf("[%s]AMF0 Read name err: %s", conn.Id(), err)
						return
					}
					transactionID, err = amf.ReadDouble(message.Buf)
					if err != nil {
						log.Printf("[%s]AMF0 Read transactionID err: %s", conn.Id(), err)
						return
					}
					cmd.TransactionID = uint32(transactionID)
					for message.Buf.Len() > 0 {
						object, err = amf.ReadValue(message.Buf)
						if err != nil {
							log.Printf("[%s]AMF0 Read object err: %s", conn.Id(), err)
							return
						}
						cmd.Objects = append(cmd.Objects, object)
					}
				default:
					log.Printf("Unkown message type %d in Command chunk stream!", message.Type)
				}
				conn.invokeCommand(cmd)
			} else {
				conn.handler.OnConnMessageReceived(conn, message)
			}
		default:
			conn.handler.OnConnMessageReceived(conn, message)
		}
	}
}

func (conn *conn) invokeSetChunkSize(message *Message) {
	if err := binary.Read(message.Buf, binary.BigEndian, &conn.inChunkSize); err != nil {
		log.Printf("[%s] invokeSetChunkSize err: %s", conn.Id(), err)
	}
	log.Printf("[%s] invokeSetChunkSize() conn.inChunkSize = %d", conn.Id(), conn.inChunkSize)
}

func (conn *conn) invokeAbortMessage(message *Message) {
	log.Printf("[%s] invokeAbortMessage: %s", conn.Id(), message.Dump(""))
}

func (conn *conn) invokeAcknowledgement(message *Message) {
	log.Printf("[%s] invokeAcknowledgement(): % 2x", conn.Id(), message.Buf.Bytes())
}

// User Control Message
//
// The client or the server sends this message to notify the peer about
// the user control events. This message carries Event type and Event
// data.
// +------------------------------+-------------------------
// |     Event Type ( 2- bytes )  | Event Data
// +------------------------------+-------------------------
// Figure 5 Pay load for the ‘User Control Message’.
//
//
// The first 2 bytes of the message data are used to identify the Event
// type. Event type is followed by Event data. Size of Event data field
// is variable.
//
//
// The client or the server sends this message to notify the peer about
// the user control events. For information about the message format,
// refer to the User Control Messages section in the RTMP Message
// Foramts draft.
//
// The following user control event types are supported:
// +---------------+--------------------------------------------------+
// |     Event     |                   Description                    |
// +---------------+--------------------------------------------------+
// |Stream Begin   | The server sends this event to notify the client |
// |        (=0)   | that a stream has become functional and can be   |
// |               | used for communication. By default, this event   |
// |               | is sent on ID 0 after the application connect    |
// |               | command is successfully received from the        |
// |               | client. The event data is 4-byte and represents  |
// |               | the stream ID of the stream that became          |
// |               | functional.                                      |
// +---------------+--------------------------------------------------+
// | Stream EOF    | The server sends this event to notify the client |
// |        (=1)   | that the playback of data is over as requested   |
// |               | on this stream. No more data is sent without     |
// |               | issuing additional commands. The client discards |
// |               | the messages received for the stream. The        |
// |               | 4 bytes of event data represent the ID of the    |
// |               | stream on which playback has ended.              |
// +---------------+--------------------------------------------------+
// | StreamDry     | The server sends this event to notify the client |
// |      (=2)     | that there is no more data on the stream. If the |
// |               | server does not detect any message for a time    |
// |               | period, it can notify the subscribed clients     |
// |               | that the stream is dry. The 4 bytes of event     |
// |               | data represent the stream ID of the dry stream.  |
// +---------------+--------------------------------------------------+
// | SetBuffer     | The client sends this event to inform the server |
// | Length (=3)   | of the buffer size (in milliseconds) that is     |
// |               | used to buffer any data coming over a stream.    |
// |               | This event is sent before the server starts      |
// |               | processing the stream. The first 4 bytes of the  |
// |               | event data represent the stream ID and the next  |
// |               | 4 bytes represent the buffer length, in          |
// |               | milliseconds.                                    |
// +---------------+--------------------------------------------------+
// | StreamIs      | The server sends this event to notify the client |
// | Recorded (=4) | that the stream is a recorded stream. The        |
// |               | 4 bytes event data represent the stream ID of    |
// |               | the recorded stream.                             |
// +---------------+--------------------------------------------------+
// | PingRequest   | The server sends this event to test whether the  |
// |       (=6)    | client is reachable. Event data is a 4-byte      |
// |               | timestamp, representing the local server time    |
// |               | when the server dispatched the command. The      |
// |               | client responds with kMsgPingResponse on         |
// |               | receiving kMsgPingRequest.                       |
// +---------------+--------------------------------------------------+
// | PingResponse  | The client sends this event to the server in     |
// |        (=7)   | response to the ping request. The event data is  |
// |               | a 4-byte timestamp, which was received with the  |
// |               | kMsgPingRequest request.                         |
// +---------------+--------------------------------------------------+
func (conn *conn) invokeUserControlMessage(message *Message) {
	var eventType uint16
	err := binary.Read(message.Buf, binary.BigEndian, &eventType)
	if err != nil {
		log.Printf("[%s] invokeUserControlMessage() read event type err: %s", conn.Id(), err.Error())
		return
	}
	switch eventType {
	case EVENT_STREAM_BEGIN:
		log.Printf("[%s] invokeUserControlMessage() EVENT_STREAM_BEGIN", conn.Id())
	case EVENT_STREAM_EOF:
		log.Printf("[%s] invokeUserControlMessage() EVENT_STREAM_EOF", conn.Id())
	case EVENT_STREAM_DRY:
		log.Printf("[%s] invokeUserControlMessage() EVENT_STREAM_DRY", conn.Id())
	case EVENT_SET_BUFFER_LENGTH:
		log.Printf("[%s] invokeUserControlMessage() EVENT_SET_BUFFER_LENGTH", conn.Id())
	case EVENT_STREAM_IS_RECORDED:
		log.Printf("[%s] invokeUserControlMessage() EVENT_STREAM_IS_RECORDED", conn.Id())
	case EVENT_PING_REQUEST:
		// Respond ping
		// Get server timestamp
		var serverTimestamp uint32
		err = binary.Read(message.Buf, binary.BigEndian, &serverTimestamp)
		if err != nil {
			log.Printf("[%s] invokeUserControlMessage() read serverTimestamp err: %s", conn.Id(), err.Error())
			return
		}
		respmessage := NewMessage(CS_ID_PROTOCOL_CONTROL, USER_CONTROL_MESSAGE, 0, message.Timestamp+1, nil)
		respEventType := uint16(EVENT_PING_RESPONSE)
		if err = binary.Write(respmessage.Buf, binary.BigEndian, &respEventType); err != nil {
			log.Printf("[%s] invokeUserControlMessage() write event type err: %s", conn.Id(), err)
			return
		}
		if err = binary.Write(respmessage.Buf, binary.BigEndian, &serverTimestamp); err != nil {
			log.Printf("[%s] invokeUserControlMessage() write streamId err: %s", conn.Id(), err)
			return
		}
		log.Printf("[%s] invokeUserControlMessage() Ping response", conn.Id())
		conn.Send(respmessage)
	case EVENT_PING_RESPONSE:
		log.Printf("[%s] invokeUserControlMessage() EVENT_PING_RESPONSE", conn.Id())
	case EVENT_REQUEST_VERIFY:
		log.Printf("[%s] invokeUserControlMessage() EVENT_REQUEST_VERIFY", conn.Id())
	case EVENT_RESPOND_VERIFY:
		log.Printf("[%s] invokeUserControlMessage() EVENT_RESPOND_VERIFY", conn.Id())
	case EVENT_BUFFER_EMPTY:
		log.Printf("[%s] invokeUserControlMessage() EVENT_BUFFER_EMPTY", conn.Id())
	case EVENT_BUFFER_READY:
		log.Printf("[%s] invokeUserControlMessage() EVENT_BUFFER_READY", conn.Id())
	default:
		log.Printf("[%s] invokeUserControlMessage() Unknown user control message :0x%x", conn.Id(), eventType)
	}
}

func (conn *conn) invokeWindowAcknowledgementSize(message *Message) {
	var size uint32
	var err error
	if err = binary.Read(message.Buf, binary.BigEndian, &size); err != nil {
		log.Printf("[%s] invokeWindowAcknowledgementSize read window size err: %s", conn.Id(), err)
		return
	}
	conn.inWindowSize = size
	log.Printf("[%s] invokeWindowAcknowledgementSize() conn.inWindowSize = %d", conn.Id(), conn.inWindowSize)
}

func (conn *conn) invokeSetPeerBandwidth(message *Message) {
	var err error
	var size uint32
	if err = binary.Read(message.Buf, binary.BigEndian, &conn.inBandwidth); err != nil {
		log.Printf("[%s] invokeSetPeerBandwidth read window size err: %s", conn.Id(), err)
		return
	}
	conn.inBandwidth = size
	var limit byte
	if limit, err = message.Buf.ReadByte(); err != nil {
		log.Printf("[%s] invokeSetPeerBandwidth read limit err: %s", conn.Id(), err)
		return
	}
	conn.inBandwidthLimit = uint8(limit)
	log.Printf("[%s] conn.inBandwidthLimit = %d/n", conn.Id(), conn.inBandwidthLimit)
}

func (conn *conn) invokeCommand(cmd *Command) {
	log.Printf("[%s] invokeCommand()", conn.Id())
	conn.handler.OnReceivedRtmpCommand(conn, cmd)
}

func (conn *conn) SetStreamBufferSize(streamId uint32, size uint32) {
	log.Printf("[%s] SetStreamBufferSize(streamId: %d, size: %d)", conn.Id(), streamId, size)
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, USER_CONTROL_MESSAGE, 0, 1, nil)
	eventType := uint16(EVENT_SET_BUFFER_LENGTH)
	if err := binary.Write(message.Buf, binary.BigEndian, &eventType); err != nil {
		log.Printf("[%s] SetStreamBufferSize write event type err: %s", conn.Id(), err)
		return
	}
	if err := binary.Write(message.Buf, binary.BigEndian, &streamId); err != nil {
		log.Printf("[%s] SetStreamBufferSize write streamId err: %s", conn.Id(), err)
		return
	}
	if err := binary.Write(message.Buf, binary.BigEndian, &size); err != nil {
		log.Printf("[%s] SetStreamBufferSize write size err: %s", conn.Id(), err)
		return
	}
	conn.Send(message)
}

func (conn *conn) SetChunkSize(size uint32) {
	log.Printf("[%s] SetChunkSize to %d", conn.Id(), size)
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, SET_CHUNK_SIZE, MS_ID_CONTROL_STREAM, 0, nil)
	if err := binary.Write(message.Buf, binary.BigEndian, &size); err != nil {
		log.Printf("[%s] SetChunkSize write event type err: %s", conn.Id(), err)
		return
	}
	conn.outChunkSizeTemp = size
	conn.Send(message)
}

func (conn *conn) SetWindowAcknowledgementSize(inWindowSize uint32, outWindowSize uint32) {
	conn.inWindowSize  = inWindowSize
	conn.outWindowSize = outWindowSize
	log.Printf("[%s] SetWindowAcknowledgementSize to %d", conn.Id(), conn.outWindowSize)
	// Request window acknowledgement size
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, WINDOW_ACKNOWLEDGEMENT_SIZE, MS_ID_CONTROL_STREAM, 0, nil)
	if err := binary.Write(message.Buf, binary.BigEndian, &conn.outWindowSize); err != nil {
		log.Printf("[%s] SetWindowAcknowledgementSize write window size err: %s", conn.Id(), err)
		return
	}
	message.Size = uint32(message.Buf.Len())
	conn.Send(message)
}
func (conn *conn) SetPeerBandwidth(peerBandwidth uint32, limitType byte) {
	log.Printf("[%s] SetPeerBandwidth to %d", conn.Id(), peerBandwidth)
	// Request window acknowledgement size
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, SET_PEER_BANDWIDTH, MS_ID_CONTROL_STREAM, 0, nil)
	if err := binary.Write(message.Buf, binary.BigEndian, &peerBandwidth); err != nil {
		log.Printf("[%s] SetPeerBandwidth write peerBandwidth err: %s", conn.Id(), err)
		return
	}
	if err := message.Buf.WriteByte(limitType); err != nil {
		log.Printf("[%s] SetPeerBandwidth write limitType err: %s", conn.Id(), err)
		return
	}
	message.Size = uint32(message.Buf.Len())
	conn.Send(message)
}

func (conn *conn) SendUserControlMessage(eventId uint16) {
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, USER_CONTROL_MESSAGE, MS_ID_CONTROL_STREAM, 0, nil)
	log.Printf("[%s] SendUserControlMessage: %s", conn.Id(), message.Dump(""))
	if err := binary.Write(message.Buf, binary.BigEndian, &eventId); err != nil {
		log.Printf("[%s] SendUserControlMessage write event type err: %s", conn.Id(), err)
		return
	}
	conn.Send(message)
}

package main

import (
	"flag"
	"fmt"
	rtmp "github.com/berndfo/gortmp"
	"os"
	"os/signal"
	"syscall"
	"log"
	"github.com/berndfo/gortmp/demo/server"
)

const (
	programName = "RtmpServer"
	version     = "0.0.1"
)

var (
	address     *string = flag.String("Address", ":1935", "The address to bind.")
)

// implements rtmp.ConnHandler
// implements rtmp.ServerConnHandler
// implements rtmp.ServerHandler
type ServerHandler struct{}

// ConnHandler methods
func (handler *ServerHandler) OnConnMessageReceived(conn rtmp.Conn, message *rtmp.Message) {
	log.Printf("OnReceived, cs id = %d, message type %d (%s)", message.ChunkStreamID, message.Type, message.TypeDisplay())
}

func (handler *ServerHandler) OnReceivedRtmpCommand(conn rtmp.Conn, command *rtmp.Command) {
	log.Printf("OnReceviedRtmpCommand: %+v", command)
}

func (handler *ServerHandler) OnClosed(conn rtmp.Conn) {
	log.Printf("OnClosed")
}

// ServerConnHandler methods
func (handler *ServerHandler) OnStatus(conn rtmp.ServerConn) {
	status, err := conn.Status()
	log.Printf("OnStatus: %d (%s), err: %v\n", status, rtmp.StatusDisplay(status), err)
}

func (handler *ServerHandler) OnStreamCreated(conn rtmp.ServerConn, stream rtmp.ServerStream) {
	log.Printf("OnStreamCreated: stream id = %d", stream.ID())
	streamHandler := server.DefaultServerStreamHandler{}
	stream.Attach(streamHandler)
}

func (handler *ServerHandler) OnStreamClosed(conn rtmp.ServerConn, stream rtmp.ServerStream) {
	log.Printf("OnStreamSlosed: stream id = %d", stream.ID())
}

// Server handler functions
func (handler *ServerHandler) NewConnection(serverConn rtmp.ServerConn, connectReq *rtmp.Command,
	server *rtmp.Server) bool {
	log.Printf("NewConnection\n")
	serverConn.Attach(handler)
	return true
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s version[%s]\r\nUsage: %s [OPTIONS]\r\n", programName, version, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	handler := &ServerHandler{}
	server, err := rtmp.NewServer("tcp", *address, handler)
	if err != nil {
		log.Println("NewServer error", err)
		os.Exit(-1)
	}
	defer server.Close()

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT)
	sig := <-ch
	log.Printf("Signal received: %v\n", sig)
}
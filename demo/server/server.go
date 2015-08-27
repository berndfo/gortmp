package main

import (
	"flag"
	"fmt"
	"github.com/berndfo/goflv"
	rtmp "github.com/berndfo/gortmp"
	"os"
	"os/signal"
	"syscall"
	"log"
	"github.com/berndfo/goamf"
)

const (
	programName = "RtmpServer"
	version     = "0.0.1"
)

var (
	address     *string = flag.String("Address", ":1935", "The address to bind.")
	flvFileName *string = flag.String("FLV", "", "Dump FLV into file.")
)

var (
	g_ibConn      rtmp.ServerConn
	videoDataSize int64
	audioDataSize int64
	flvFile       *flv.File
	status        uint
)

// implements rtmp.ConnHandler
// implements rtmp.ServerConnHandler
// implements rtmp.ServerStreamHandler
// implements rtmp.ServerHandler
type ServerHandler struct{}

// ConnHandler methods
func (handler *ServerHandler) OnClosed(conn rtmp.Conn) {
	log.Printf("OnClosed")
}

func (handler *ServerHandler) OnReceived(conn rtmp.Conn, message *rtmp.Message) {
	//log.Printf("OnReceived, cs id = %d, message type %d (%s)", message.ChunkStreamID, message.Type, message.TypeDisplay())
}

func (handler *ServerHandler) OnReceivedRtmpCommand(conn rtmp.Conn, command *rtmp.Command) {
	log.Printf("OnReceviedRtmpCommand: %+v", command)
}

// ServerConnHandler methods
func (handler *ServerHandler) OnStatus(conn rtmp.ServerConn) {
	status, err := g_ibConn.Status()
	log.Printf("OnStatus: %d, err: %v\n", status, err)
}

func (handler *ServerHandler) OnStreamCreated(conn rtmp.ServerConn, stream rtmp.ServerStream) {
	log.Printf("OnStreamCreated: stream id = %d", stream.ID())
	stream.Attach(handler)
}

func (handler *ServerHandler) OnStreamClosed(conn rtmp.ServerConn, stream rtmp.ServerStream) {
	log.Printf("OnStreamSlosed: stream id = %d", stream.ID())
}

// ServerStreamHandler methods
func (handler *ServerHandler) OnPlayStart(stream rtmp.ServerStream) {
	log.Printf("OnPlayStart")
}

func (handler *ServerHandler) OnPublishStart(stream rtmp.ServerStream, publishingName string, publishingType string) {
	log.Printf("OnPublishStart requested by client for name = %q, type = %q", publishingName, publishingType)
	go func() {
		// TODO: decide if this request will be accepted at all
		
		netStreamUpstream, err := rtmp.RegisterNewNetStream(publishingName, publishingType, stream)
		if err != nil {
			// TODO return different, appropriate status message 
			return
		}
		
		// TODO remove hard-coded file recording
		recorderDownstream := rtmp.CreateFileRecorder(publishingName + ".flv", netStreamUpstream.Info())
		rtmp.RegisterDownstream(netStreamUpstream.Info().Name, &recorderDownstream)
		
		stream.Attach(rtmp.NetStreamDispatchingHandler{})
		
		message := rtmp.NewMessage(stream.ChunkStreamID(), rtmp.COMMAND_AMF0, stream.ID(), 0, nil)
		amf.WriteString(message.Buf, "onStatus")
		amf.WriteDouble(message.Buf, 0)
		amf.WriteNull(message.Buf)
		amf.WriteObject(message.Buf, amf.Object{
			"level":       "status",
			"code":        rtmp.NETSTREAM_PUBLISH_START,
			"description":  "start publishing!",
		})
		message.Dump("onpublishstart accept:")
		

		stream.Conn().Conn().Send(message)
	}()
}
func (handler *ServerHandler) OnReceiveAudio(stream rtmp.ServerStream, requestingData bool) {
	log.Printf("OnReceiveAudio: %b", requestingData)
}
func (handler *ServerHandler) OnReceiveVideo(stream rtmp.ServerStream, requestingData bool) {
	log.Printf("OnReceiveVideo: %b", requestingData)
}

// Server handler functions
func (handler *ServerHandler) NewConnection(ibConn rtmp.ServerConn, connectReq *rtmp.Command,
	server *rtmp.Server) bool {
	log.Printf("NewConnection\n")
	ibConn.Attach(handler)
	g_ibConn = ibConn
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
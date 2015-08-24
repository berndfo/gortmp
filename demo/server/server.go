package main

import (
	"flag"
	"fmt"
	"github.com/berndfo/goflv"
	rtmp "github.com/berndfo/gortmp"
	"os"
	"os/signal"
	"syscall"
	"time"
	"log"
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
	g_ibConn      rtmp.InboundConn
	videoDataSize int64
	audioDataSize int64
	flvFile       *flv.File
	status        uint
)

type ServerHandler struct{}

// InboundConn handler funcions
func (handler *ServerHandler) OnStatus(conn rtmp.InboundConn) {
	status, err := g_ibConn.Status()
	log.Printf("@@@@@@@@@@@@@status: %d, err: %v\n", status, err)
}

func (handler *ServerHandler) OnStreamCreated(conn rtmp.InboundConn, stream rtmp.InboundStream) {
	log.Printf("StreamCreated: %d", stream.ID())
	stream.Attach(handler)
}

func (handler *ServerHandler) OnStreamClosed(conn rtmp.InboundConn, stream rtmp.InboundStream) {
	log.Printf("OnStreamSlosed: %d", stream.ID())
}

// Conn handler functions
func (handler *ServerHandler) OnClosed(conn rtmp.Conn) {
	log.Printf("OnClosed")
}

func (handler *ServerHandler) OnReceived(conn rtmp.Conn, message *rtmp.Message) {
	log.Printf("OnReceived")
}

func (handler *ServerHandler) OnReceivedRtmpCommand(conn rtmp.Conn, command *rtmp.Command) {
	log.Printf("ReceviedRtmpCommand: %+v\n", command)
}

// Stream handle functions
func (handler *ServerHandler) OnPlayStart(stream rtmp.InboundStream) {
	log.Printf("OnPlayStart\n")
	go publish(stream)
}
func (handler *ServerHandler) OnPublishStart(stream rtmp.InboundStream) {
	log.Printf("OnPublishStart\n")
}
func (handler *ServerHandler) OnReceiveAudio(stream rtmp.InboundStream, on bool) {
	log.Printf("OnReceiveAudio: %b\n", on)
}
func (handler *ServerHandler) OnReceiveVideo(stream rtmp.InboundStream, on bool) {
	log.Printf("OnReceiveVideo: %b\n", on)
}

// Server handler functions
func (handler *ServerHandler) NewConnection(ibConn rtmp.InboundConn, connectReq *rtmp.Command,
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

func publish(stream rtmp.InboundStream) {

	var err error
	flvFile, err = flv.OpenFile(*flvFileName)
	if err != nil {
		log.Println("Open FLV dump file error:", err)
		return
	}
	defer flvFile.Close()
	startTs := uint32(0)
	startAt := time.Now().UnixNano()
	preTs := uint32(0)
	for {
		if status, _ = g_ibConn.Status(); status != rtmp.INBOUND_CONN_STATUS_CREATE_STREAM_OK {
			break
		}
		if flvFile.IsFinished() {
			log.Println("@@@@@@@@@@@@@@File finished")
			flvFile.LoopBack()
			startAt = time.Now().UnixNano()
			startTs = uint32(0)
			preTs = uint32(0)
		}
		header, data, err := flvFile.ReadTag()
		if err != nil {
			log.Println("flvFile.ReadTag() error:", err)
			break
		}
		switch header.TagType {
		case flv.VIDEO_TAG:
			videoDataSize += int64(len(data))
		case flv.AUDIO_TAG:
			audioDataSize += int64(len(data))
		}

		if startTs == uint32(0) {
			startTs = header.Timestamp
		}
		diff1 := uint32(0)
		//		deltaTs := uint32(0)
		if header.Timestamp > startTs {
			diff1 = header.Timestamp - startTs
		} else {
			log.Println("@@@@@@@@@@@@@@diff1")
		}
		if diff1 > preTs {
			//			deltaTs = diff1 - preTs
			preTs = diff1
		}
		if err = stream.SendData(header.TagType, data, diff1); err != nil {
			log.Println("PublishData() error:", err)
			break
		}
		diff2 := uint32((time.Now().UnixNano() - startAt) / 1000000)
		//		log.Printf("diff1: %d, diff2: %d\n", diff1, diff2)
		if diff1 > diff2+100 {
			//			log.Printf("header.Timestamp: %d, now: %d\n", header.Timestamp, time.Now().UnixNano())
			time.Sleep(time.Millisecond * time.Duration(diff1-diff2))
		}
	}
}

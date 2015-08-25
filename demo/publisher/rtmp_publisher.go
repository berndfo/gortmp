package main

import (
	"flag"
	"fmt"
	"github.com/berndfo/goflv"
	rtmp "github.com/berndfo/gortmp"
	"os"
	"time"
	"log"
)

const (
	programName = "RtmpPublisher"
	version     = "0.0.1"
)

var (
	url         *string = flag.String("URL", "rtmp://localhost:1935/stream", "The rtmp url to connect.")
	streamName  *string = flag.String("Stream", "camstream", "Stream name to play.")
	flvFileName *string = flag.String("FLV", "", "published FLV file.")
)

var obConn rtmp.OutboundConn
var createStreamChan chan rtmp.OutboundStream
var videoDataSize int64
var audioDataSize int64
var flvFile *flv.File

var status uint

type PublishingOutboundConnHandler struct {
}

func (handler *PublishingOutboundConnHandler) OnStatus(conn rtmp.OutboundConn) {
	var err error
	status, err = obConn.Status()
	log.Printf("OnStatus: %d, err: %v\n", status, err)
}

func (handler *PublishingOutboundConnHandler) OnClosed(conn rtmp.Conn) {
	log.Println("OnClosed")
}

func (handler *PublishingOutboundConnHandler) OnReceived(conn rtmp.Conn, message *rtmp.Message) {
	log.Println("OnReceived")
	message.Dump("fromserver")
}

func (handler *PublishingOutboundConnHandler) OnReceivedRtmpCommand(conn rtmp.Conn, command *rtmp.Command) {
	log.Printf("OnReceivedRtmpCommand: %+v", command)
}

func (handler *PublishingOutboundConnHandler) OnStreamCreated(conn rtmp.OutboundConn, stream rtmp.OutboundStream) {
	log.Printf("OnStreamCreated, id = %d", stream.ID())
	createStreamChan <- stream
}
func (handler *PublishingOutboundConnHandler) OnPlayStart(stream rtmp.OutboundStream) {
	log.Println("OnPlayStart")
}
func (handler *PublishingOutboundConnHandler) OnPublishStart(stream rtmp.OutboundStream) {
	log.Println("OnPublishStart")
	go publish(stream)
}

func publish(stream rtmp.OutboundStream) {

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
	for status == rtmp.OUTBOUND_CONN_STATUS_CREATE_STREAM_OK {
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
		if err = stream.PublishData(header.TagType, data, diff1); err != nil {
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

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s version[%s]\r\nUsage: %s [OPTIONS]\r\n", programName, version, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	createStreamChan = make(chan rtmp.OutboundStream)
	handler := &PublishingOutboundConnHandler{}
	log.Printf("dialing %v", url)
	var err error
	obConn, err = rtmp.Dial(*url, handler, 100)
	if err != nil {
		log.Println("Dial error", err)
		os.Exit(-1)
	}
	defer obConn.Close()

	log.Printf("connecting %v", url)
	err = obConn.Connect()
	if err != nil {
		log.Printf("Connect error: %s", err.Error())
		os.Exit(-1)
	}
	log.Printf("connected to %v", url)
	
	for {
		select {
		case stream := <-createStreamChan:
			// Publish
			log.Printf("starting publishing on stream %d", stream.ID())
			stream.Attach(handler)
			err = stream.Publish(*streamName, "live")
			if err != nil {
				log.Printf("Publish error: %s", err.Error())
				os.Exit(-1)
			}

		case <-time.After(10 * time.Second):
			log.Printf("Audio size: %d bytes; Video size: %d bytes\n", audioDataSize, videoDataSize)
		}
	}
}

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
	programName = "RtmpPlayer"
	version     = "0.0.1"
)

var (
	url        *string = flag.String("URL", "rtmp://192.168.20.111/vid3", "The rtmp url to connect.")
	streamName *string = flag.String("Stream", "camstream", "Stream name to play.")
	dumpFlv    *string = flag.String("DumpFLV", "", "Dump FLV into file.")
)

type TestClientConnHandler struct {
}

var obConn rtmp.ClientConn
var createStreamChan chan rtmp.ClientStream
var videoDataSize int64
var audioDataSize int64
var flvFile *flv.File
var status uint

func (handler *TestClientConnHandler) OnStatus(conn rtmp.ClientConn) {
	var err error
	status, err = obConn.Status()
	log.Printf("@@@@@@@@@@@@@status: %d, err: %v\n", status, err)
}

func (handler *TestClientConnHandler) OnClosed(conn rtmp.Conn) {
	log.Printf("@@@@@@@@@@@@@Closed\n")
}

func (handler *TestClientConnHandler) OnConnMessageReceived(conn rtmp.Conn, message *rtmp.Message) {
	switch message.Type {
	case rtmp.VIDEO_TYPE:
		if flvFile != nil {
			flvFile.WriteVideoTag(message.Buf.Bytes(), message.AbsoluteTimestamp)
		}
		videoDataSize += int64(message.Buf.Len())
	case rtmp.AUDIO_TYPE:
		if flvFile != nil {
			flvFile.WriteAudioTag(message.Buf.Bytes(), message.AbsoluteTimestamp)
		}
		audioDataSize += int64(message.Buf.Len())
	}
}

func (handler *TestClientConnHandler) OnReceivedRtmpCommand(conn rtmp.Conn, command *rtmp.Command) {
	log.Printf("ReceviedCommand: %+v\n", command)
}

func (handler *TestClientConnHandler) OnStreamCreated(conn rtmp.ClientConn, stream rtmp.ClientStream) {
	log.Printf("Stream created: %d\n", stream.ID())
	createStreamChan <- stream
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s version[%s]\r\nUsage: %s [OPTIONS]\r\n", programName, version, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Create flv file
	if len(*dumpFlv) > 0 {
		var err error
		flvFile, err = flv.CreateFile(*dumpFlv)
		if err != nil {
			log.Println("Create FLV dump file error:", err)
			return
		}
	}
	defer func() {
		if flvFile != nil {
			flvFile.Close()
		}
	}()

	createStreamChan = make(chan rtmp.ClientStream, 50)
	testHandler := &TestClientConnHandler{}
	log.Println("to dial")

	var err error

	obConn, err = rtmp.Dial(*url, testHandler, 100)
	/*
		conn := TryHandshakeByVLC()
		obConn, err = rtmp.NewOutbounConn(conn, *url, testHandler, 100)
	*/
	if err != nil {
		log.Println("Dial error", err)
		os.Exit(-1)
	}

	defer obConn.Close()
	log.Printf("obConn: %+v\n", obConn)
	log.Printf("obConn.URL(): %s\n", obConn.URL())
	log.Println("to connect")
	//	err = obConn.Connect("33abf6e996f80e888b33ef0ea3a32bfd", "131228035", "161114738", "play", "", "", "1368083579")
	err = obConn.Connect()
	if err != nil {
		log.Printf("Connect error: %s", err.Error())
		os.Exit(-1)
	}
	for {
		select {
		case stream := <-createStreamChan:
			// Play
			err = stream.Play(*streamName, float32(-1), float32(-2), false)
			if err != nil {
				log.Printf("Play error: %s", err.Error())
				os.Exit(-1)
			}
			// Set Buffer Length

		case <-time.After(1 * time.Second):
			log.Printf("Audio size: %d bytes; Video size: %d bytes\n", audioDataSize, videoDataSize)
		}
	}
}
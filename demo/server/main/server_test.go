package main
import (
	"testing"
	"log"
	"os"
	rtmp "github.com/berndfo/gortmp"
	"fmt"
	"time"
	"encoding/hex"
)

var LOCALSERVER string = "rtmp://localhost:1935/"

var publishCreateStreamChan, publishPublishStartChan, playCreateStreamChan chan rtmp.ClientStream
var playMsgReceiver chan *rtmp.Message
var playCmdReceiver chan *rtmp.Command

type PublishingClientConnHandler struct {
}

func (handler *PublishingClientConnHandler) OnStatus(conn rtmp.ClientConn) {
	var err error
	var status uint
	status, err = conn.Status()
	log.Printf("pub:OnStatus: %d (%s), err: %v\n", status, rtmp.StatusDisplay(status), err)
}

func (handler *PublishingClientConnHandler) OnClosed(conn rtmp.Conn) {
	log.Println("pub:OnClosed")
}

func (handler *PublishingClientConnHandler) OnConnMessageReceived(conn rtmp.Conn, message *rtmp.Message) {
	log.Println("pub:OnConnMessageReceived")
}

func (handler *PublishingClientConnHandler) OnReceivedRtmpCommand(conn rtmp.Conn, command *rtmp.Command) {
	log.Printf("pub:OnReceivedRtmpCommand: %+v", command)
}

func (handler *PublishingClientConnHandler) OnStreamCreated(conn rtmp.ClientConn, stream rtmp.ClientStream) {
	log.Printf("pub:OnStreamCreated, id = %d", stream.ID())
	publishCreateStreamChan <- stream
}
func (handler *PublishingClientConnHandler) OnPlayStart(stream rtmp.ClientStream) {
	log.Println("pub:OnPlayStart")
}
func (handler *PublishingClientConnHandler) OnPublishStart(stream rtmp.ClientStream) {
	log.Println("pub:OnPublishStart")
	publishPublishStartChan <- stream
}

type PlayingClientConnHandler struct {
}

func (handler *PlayingClientConnHandler) OnStatus(conn rtmp.ClientConn) {
	var err error
	var status uint
	status, err = conn.Status()
	log.Printf("play:OnStatus: %d (%s), err: %v\n", status, rtmp.StatusDisplay(status), err)
}

func (handler *PlayingClientConnHandler) OnClosed(conn rtmp.Conn) {
	log.Println("play:OnClosed")
}

func (handler *PlayingClientConnHandler) OnConnMessageReceived(conn rtmp.Conn, message *rtmp.Message) {
	log.Println("play:OnConnMessageReceived")
	playMsgReceiver <-message
}

func (handler *PlayingClientConnHandler) OnReceivedRtmpCommand(conn rtmp.Conn, command *rtmp.Command) {
	log.Printf("play:OnReceivedRtmpCommand: %+v", command)
	playCmdReceiver <-command
}

func (handler *PlayingClientConnHandler) OnStreamCreated(conn rtmp.ClientConn, stream rtmp.ClientStream) {
	log.Printf("play:OnStreamCreated, id = %d", stream.ID())
	playCreateStreamChan <- stream
}
func (handler *PlayingClientConnHandler) OnPlayStart(stream rtmp.ClientStream) {
	log.Println("play:OnPlayStart")
}
func (handler *PlayingClientConnHandler) OnPublishStart(stream rtmp.ClientStream) {
	log.Println("play:OnPublishStart")
}


func TestEndToEnd(t *testing.T) {
	
	publishCreateStreamChan = make(chan rtmp.ClientStream, 50)
	publishPublishStartChan = make(chan rtmp.ClientStream, 50)
	playCreateStreamChan = make(chan rtmp.ClientStream, 50)
	playMsgReceiver = make(chan *rtmp.Message, 50)
	playCmdReceiver = make(chan *rtmp.Command, 50)
	
	pubHandler := &PublishingClientConnHandler{}
	log.Printf("pub: dialing %v", LOCALSERVER)
	var err error
	var pubConn rtmp.ClientConn
	pubConn, err = rtmp.Dial(LOCALSERVER, pubHandler, 100)
	if err != nil {
		log.Println("Dial error", err)
		os.Exit(-1)
	}
	defer pubConn.Close()
	
	pubConn.Connect()
	if err != nil {
		log.Printf("Connect error: %s", err.Error())
		os.Exit(-1)
	}
	log.Printf("pub:connected to %v", LOCALSERVER)
	
	log.Println("pub: waiting for publish control stream")
	pubStream := <-publishCreateStreamChan
	pubStream.Attach(pubHandler)
	
	playHandler := &PlayingClientConnHandler{}
	log.Printf("play: dialing %v", LOCALSERVER)
	var playConn rtmp.ClientConn
	playConn, err = rtmp.Dial(LOCALSERVER, playHandler, 100)
	if err != nil {
		log.Println("Dial error", err)
		os.Exit(-1)
	}
	defer playConn.Close()
	
	playConn.Connect()
	if err != nil {
		log.Printf("Connect error: %s", err.Error())
		os.Exit(-1)
	}
	log.Printf("play: connected to %v", LOCALSERVER)
	
	log.Println("play: waiting for play control stream")
	playStream := <-playCreateStreamChan
	playStream.Attach(playHandler)
	
	// create publishing side
	streamName := fmt.Sprintf("sn_%d", time.Now().UnixNano()) 
	err = pubStream.Publish(streamName, "live")
	if err != nil {
		log.Printf("Publish command error: %s", err.Error())
		os.Exit(-1)
	}
	<-publishPublishStartChan 
	
	// create playing side
	err = playStream.Play(streamName, float32(-2), float32(-1), true)
	if err != nil {
		log.Printf("Play command error: %s", err.Error())
		os.Exit(-1)
	}
	
	<-playMsgReceiver
	<-playMsgReceiver
	<-playMsgReceiver
	
	pubStream.PublishVideoData([]byte{1,2,3}, uint32(time.Now().Second()))
	
	for {
		select {
		case msg := <-playMsgReceiver:
			msg.LogDump("## msg received:")
			log.Println(hex.Dump(msg.Buf.Bytes()))
		case cmd := <-playCmdReceiver:
			cmd.LogDump("## cmd received:")
		case <-time.After(10* time.Second):
			t.Fail()
			break;
		}
	}
	
	
	
}

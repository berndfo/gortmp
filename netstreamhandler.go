package gortmp
import (
	"log"
	"sync"
)

// implements ServerStreamHandler
type NetStreamDispatchingHandler struct {
	upstream chan<- *Message  
}

func (handler *NetStreamDispatchingHandler) OnPlayStart(stream ServerStream, name string, peerName string, start float64, duration float64, flushPrevPlaylist bool) {
	log.Println("NetStreamDispatchingHandler: 'OnPlayStart' command unhandled")
}

func (handler *NetStreamDispatchingHandler) OnPublishStart(stream ServerStream, publishingName string, publishingType string) {
	log.Println("NetStreamDispatchingHandler: 'OnPublishStart' command unhandled")
}

func (handler *NetStreamDispatchingHandler) OnReceiveAudio(stream ServerStream, on bool) {
	log.Println("NetStreamDispatchingHandler: 'receive audio' command unhandled")
}

func (handler *NetStreamDispatchingHandler) OnReceiveVideo(stream ServerStream, on bool) {
	log.Println("NetStreamDispatchingHandler: 'receive video' command unhandled")
}

func (handler *NetStreamDispatchingHandler) OnAudioData(stream ServerStream, audio *Message) {
	onceLogVideoData.Do(func () {
		log.Println("NetStreamDispatchingHandler: 'OnAudioData' handled")
	})
	handler.upstream <- audio
}

var onceLogVideoData sync.Once
func (handler *NetStreamDispatchingHandler) OnVideoData(stream ServerStream, video *Message) {
	onceLogVideoData.Do(func () {
		log.Println("NetStreamDispatchingHandler: 'OnVideoData' handled")
	})
	handler.upstream <- video
}


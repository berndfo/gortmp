package gortmp
import "log"

// implements ServerStreamHandler
type NetStreamDispatchingHandler struct {
}

func (handler NetStreamDispatchingHandler) OnPlayStart(stream ServerStream) {
	
}

func (handler NetStreamDispatchingHandler) OnPublishStart(stream ServerStream, publishingName string, publishingType string) {
	
}

func (handler NetStreamDispatchingHandler) OnReceiveAudio(stream ServerStream, on bool) {
	log.Println("NetStreamDispatchingHandler: received audio for dispatching")
}

func (handler NetStreamDispatchingHandler) OnReceiveVideo(stream ServerStream, on bool) {
	log.Println("NetStreamDispatchingHandler: received video for dispatching")
}

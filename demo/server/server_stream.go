package server
import (
	rtmp "github.com/berndfo/gortmp"
	"log"
	"github.com/berndfo/goamf"
	"sync"
)

// implements ServerStreamHandler
type DefaultServerStreamHandler struct{
	videoDataSize int64
	audioDataSize int64
}

func (handler DefaultServerStreamHandler) OnPlayStart(stream rtmp.ServerStream) {
	log.Printf("OnPlayStart")
}

func (handler DefaultServerStreamHandler) OnPublishStart(stream rtmp.ServerStream, publishingName string, publishingType string) {
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
func (handler DefaultServerStreamHandler) OnReceiveAudio(stream rtmp.ServerStream, requestingData bool) {
	log.Printf("OnReceiveAudio: %b", requestingData)
}
func (handler DefaultServerStreamHandler) OnReceiveVideo(stream rtmp.ServerStream, requestingData bool) {
	log.Printf("OnReceiveVideo: %b", requestingData)
}

func (handler DefaultServerStreamHandler) OnAudioData(stream rtmp.ServerStream, audio *rtmp.Message) {
	log.Println("DefaultServerStreamHandler: 'OnAudioData' message unhandled")
}

var onceLogVideoData sync.Once

func (handler DefaultServerStreamHandler) OnVideoData(stream rtmp.ServerStream, video *rtmp.Message) {
	onceLogVideoData.Do(func () {
		log.Println("DefaultServerStreamHandler: 'OnVideoData' message unhandled !!!!!")
	})
}



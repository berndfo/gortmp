package server
import (
	rtmp "github.com/berndfo/gortmp"
	"log"
	"github.com/berndfo/goamf"
	"sync"
	"time"
)

// implements NetStreamDownstream
type downstreamer struct {
	info rtmp.NetStreamInfo
	channel chan<- *rtmp.Message
}

func (d *downstreamer) Info() rtmp.NetStreamInfo {
	return d.info
}
func (d *downstreamer) PushDownstream(msg *rtmp.Message) {
	d.channel<-msg
}

// implements ServerStreamHandler
type DefaultServerStreamHandler struct{
	videoDataSize int64
	audioDataSize int64
}

func (handler *DefaultServerStreamHandler) OnPlayStart(stream rtmp.ServerStream, name string, start float64, duration float64, flushPrevPlaylist bool) {
	log.Printf("OnPlayStart requested by client for name=%q, start=%f, duration=%f, flush=%t", name, start, duration, flushPrevPlaylist)
	
	info, exists := rtmp.FindNetStream(name)
	if (!exists) {
		// TODO return appropriate answer to client
		log.Printf("net stream %q not found for play", name)
		return
	}

	channel := make(chan *rtmp.Message, 100)
	
	downstreamer := downstreamer{info, channel}
	
	go func() {
		defer func() {
			log.Println("stopped relaying messages to downstream")
			return
		} ()

		var downstreamedMessages int
		// TODO remove, or it will never end
		go func() {
			for {
				select {
					case <-time.After(5*time.Second):
						log.Printf("downstream relay: processed %d messages", downstreamedMessages)
				}
			}
		}()

		for {
			select {
				case msg := <-channel:
					if (msg == nil) {
						return
					}
				
					msgCopy := rtmp.CopyToStream(stream, msg)
				
					stream.Conn().Send(msgCopy)
					downstreamedMessages++
				
				case <-time.After(10*time.Minute):
					log.Printf("no messages after 10 mins, downstreaming %q", name)
			}
		}
	}()
	
	err := rtmp.RegisterDownstream(name, &downstreamer)
	if err != nil {
		channel<-nil
		log.Printf("error registering downstream for NetStream %q", name)
	}
}

func (handler *DefaultServerStreamHandler) OnPublishStart(stream rtmp.ServerStream, publishingName string, publishingType string) {
	log.Printf("OnPublishStart requested by client for name = %q, type = %q", publishingName, publishingType)

	// TODO: decide if this request will be accepted at all
	
	netStreamUpstream, dispatcherHandler, err := rtmp.RegisterNewNetStream(publishingName, publishingType, stream)
	if err != nil {
		// TODO return different, appropriate status message
		log.Printf("error creating registering new net stream %q/%s - upstream: %s", publishingName, publishingType, err.Error())
		return
	}
	
	// TODO remove hard-coded file recording
	recorderDownstream, err := rtmp.CreateFileRecorder(publishingName + ".flv", netStreamUpstream.Info())
	if err != nil {
		log.Printf("error creating flv file for writing: %s", err.Error())
		return
	}
	err = rtmp.RegisterDownstream(netStreamUpstream.Info().Name, recorderDownstream)
	if err != nil {
		log.Printf("error creating registering new net stream - downstream")
		return
	}
	
	stream.Attach(dispatcherHandler)
	
	message := rtmp.NewMessage(stream.ChunkStreamID(), rtmp.COMMAND_AMF0, stream.ID(), 0, nil)
	amf.WriteString(message.Buf, "onStatus")
	amf.WriteDouble(message.Buf, 0)
	amf.WriteNull(message.Buf)
	amf.WriteObject(message.Buf, amf.Object{
		"level":       "status",
		"code":        rtmp.NETSTREAM_PUBLISH_START,
		"description":  "start publishing!",
	})
	message.LogDump("onpublishstart accept:")
	

	stream.Conn().Conn().Send(message)
}
func (handler *DefaultServerStreamHandler) OnReceiveAudio(stream rtmp.ServerStream, requestingData bool) {
	log.Printf("OnReceiveAudio: %t", requestingData)
}
func (handler *DefaultServerStreamHandler) OnReceiveVideo(stream rtmp.ServerStream, requestingData bool) {
	log.Printf("OnReceiveVideo: %t", requestingData)
}

var onceLogAudioData sync.Once
func (handler *DefaultServerStreamHandler) OnAudioData(stream rtmp.ServerStream, audio *rtmp.Message) {
	onceLogAudioData.Do(func () {
		log.Println("DefaultServerStreamHandler: 'OnAudioData' message unhandled")
	})
}

var onceLogVideoData sync.Once
func (handler *DefaultServerStreamHandler) OnVideoData(stream rtmp.ServerStream, video *rtmp.Message) {
	onceLogVideoData.Do(func () {
		log.Println("DefaultServerStreamHandler: 'OnVideoData' message unhandled !!!!!")
	})
}



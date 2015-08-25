package gortmp
import (
	"time"
	"log"
	"github.com/berndfo/goflv"
)


// implements interface NetStreamDownstream
type fileRecorder struct {
	info *NetStreamInfo
	msgChannel chan *Message
	flvFile *flv.File
	videoDataSize int64
	audioDataSize int64
}

func (rec fileRecorder) Info() *NetStreamInfo {
	return rec.info
}

func (rec fileRecorder) Downstream() chan<- *Message {
	return rec.msgChannel
}

func (rec fileRecorder) recordMessage(msg *Message) {
	switch msg.Type {
	case VIDEO_TYPE:
		if rec.flvFile != nil {
			rec.flvFile.WriteVideoTag(msg.Buf.Bytes(), msg.Timestamp)
		}
		rec.videoDataSize += int64(msg.Buf.Len())
	case AUDIO_TYPE:
		if rec.flvFile != nil {
			rec.flvFile.WriteAudioTag(msg.Buf.Bytes(), msg.Timestamp)
		}
		rec.audioDataSize += int64(msg.Buf.Len())
	default:
		log.Println("recordMessage: not handling message type", msg.TypeDisplay())
	}
}

func CreateFileRecorder(filename string, info *NetStreamInfo) NetStreamDownstream {

	channel := make(chan *Message, 100)

	var flvFile *flv.File
	
	recorder := fileRecorder {
		info: info,
		msgChannel: channel,
		flvFile: flvFile,
	}

	go func() {
		select {
			case msg := <-channel:
				recorder.recordMessage(msg)
			
			case <-time.After(10*time.Minute):
				log.Println("no messages after 10 mins")
		}
	}()

	return recorder 
}

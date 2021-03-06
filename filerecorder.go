package gortmp
import (
	"time"
	"log"
	"github.com/berndfo/goflv"
)


// implements interface NetStreamDownstream
// implements io.Closer
type fileRecorder struct {
	info NetStreamInfo
	msgChannel chan *Message
	flvFile *flv.File
	videoDataSize int64
	audioDataSize int64
}

func (rec *fileRecorder) Info() NetStreamInfo {
	return rec.info
}

func (rec *fileRecorder) PushDownstream(msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = DownstreamClosed
		}
	} () 
	
	rec.msgChannel<-msg
	
	return
}

func (rec *fileRecorder) Close() (err error) {
	rec.flvFile.Close()
	return
}

func (rec *fileRecorder) recordMessage(msg *Message) {
	switch msg.Type {
	case VIDEO_TYPE:
		if rec.flvFile != nil {
			rec.flvFile.WriteVideoTag(msg.Buf.Bytes(), msg.AbsoluteTimestamp)
		}
		rec.videoDataSize += int64(msg.Buf.Len())
	case AUDIO_TYPE:
		if rec.flvFile != nil {
			rec.flvFile.WriteAudioTag(msg.Buf.Bytes(), msg.AbsoluteTimestamp)
		}
		rec.audioDataSize += int64(msg.Buf.Len())
	default:
		log.Println("recordMessage: not handling message type", msg.TypeDisplay())
	}
}

func CreateFileRecorder(filename string, info NetStreamInfo) (nsd NetStreamDownstream, err error) {

	channel := make(chan *Message, 100)

	var flvFile *flv.File
	
	//filename = "aaaaaa_" + filename
	
	flvFile, err = flv.CreateFile(filename)
	if err != nil {
		return nil, err
	}
	log.Printf("filerecorder: closing file %s", filename)
	
	recorder := &fileRecorder {
		info: info,
		msgChannel: channel,
		flvFile: flvFile,
	}

	go func() {
		for {
			select {
				case msg := <-channel:
					if (msg == nil) {
						log.Printf("filerecorder: closing recorder %s", filename)
						recorder.Close()
						return
					}
					recorder.recordMessage(msg)
				
				case <-time.After(10*time.Minute):
					log.Println("filerecorder: no messages after 10 mins")
			
				case <-time.After(60*time.Minute):
					log.Printf("filerecorder: no messages after 1 hour, closing %s", filename)
					flvFile.Close()
					return
			}
		}
	}()

	return recorder, nil
}

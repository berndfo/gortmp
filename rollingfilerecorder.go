package gortmp
import (
	"time"
	"log"
	"github.com/berndfo/goflv"
)


// implements interface NetStreamDownstream
// implements io.Closer
type rollingFileRecorder struct {
	info NetStreamInfo
	msgChannel chan *Message
	flvFile *flv.File
	videoDataSize int64
	audioDataSize int64
	lastOpen time.Time
}

func (rec *rollingFileRecorder) Open() (err error) {
	var flvFile *flv.File
	
	now := time.Now()
	timePostfix := now.Format("20060102-150405")
	
	filename := rec.info.Name + "_" + timePostfix + ".flv"
	
	flvFile, err = flv.CreateFile(filename)
	if err != nil {
		return
	}
	log.Printf("filerecorder: opening file %s", filename)
	rec.lastOpen = now
	
	if rec.flvFile != nil {
		rec.flvFile.Close()
	}
	rec.flvFile = flvFile
	
	return
}

func (rec *rollingFileRecorder) Info() NetStreamInfo {
	return rec.info
}

func (rec *rollingFileRecorder) PushDownstream(msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = DownstreamClosed
		}
	} () 
	
	rec.msgChannel<-msg
	
	return
}

func (rec *rollingFileRecorder) Close() (err error) {
	rec.flvFile.Close()
	return
}

func (rec *rollingFileRecorder) checkAndReopen() {
	if time.Since(rec.lastOpen) >= time.Duration(1*time.Minute) {
		rec.Open()
	}
}
func (rec *rollingFileRecorder) recordMessage(msg *Message) {
	switch msg.Type {
	case VIDEO_TYPE:
		if rec.flvFile != nil {
			frameType, _ := flv.VideoMetaData(msg.Buf.Bytes())
			if frameType == 1 {
				rec.checkAndReopen()
			}
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

func CreateRollingFileRecorder(info NetStreamInfo) (nsd NetStreamDownstream, err error) {

	channel := make(chan *Message, 100)

	recorder := &rollingFileRecorder {
		info: info,
		msgChannel: channel,
	}
	
	err = recorder.Open()
	if err != nil {
		return
	}

	go func() {
		for {
			select {
				case msg := <-channel:
					if (msg == nil) {
						log.Printf("filerecorder: closing recorder %s", info.Name)
						recorder.Close()
						return
					}
					recorder.recordMessage(msg)
				
				case <-time.After(10*time.Minute):
					log.Println("filerecorder: no messages after 10 mins")
			
				case <-time.After(60*time.Minute):
					log.Printf("filerecorder: no messages after 1 hour, closing %s", info.Name)
					recorder.Close()
					return
			}
		}
	}()

	return recorder, nil
}

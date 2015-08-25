package gortmp
import (
	"errors"
)

var netStreams map[string]netStream = make(map[string]netStream)

type netStream struct {
	info *NetStreamInfo
	upstream *NetStreamUpstream
	downstreams []*NetStreamDownstream
}

var ErrorNameAlreadyExists error = errors.New("NameAlreadyExists") 
var StreamNotExists error = errors.New("StreamNotExists") 

func RegisterNewNetStream(name string, streamType string, inboundStream InboundStream) (info *NetStreamInfo, err error) {
	if _, exists := netStreams[name]; exists {
		return nil, ErrorNameAlreadyExists
	}
	
	info = &NetStreamInfo{
		Name: name,
		Type: streamType,
		Stream: inboundStream,
	}

	ns := netStream {
		info: info,
		downstreams: make([]*NetStreamDownstream, 0),
	}
	
	_ = ns
	
	return
}

func RegisterDownstream(name string, downstream *NetStreamDownstream) error {
	netstream, exists := netStreams[name]
	if (!exists) {
		return StreamNotExists
	}
	netstream.downstreams = append(netstream.downstreams, downstream)
	return nil
}

type NetStreamInfo struct {
	Name string
	Type string // "live", "record" or "append"
	Stream InboundStream
}

type NetStreamUpstream interface {
	Info() *NetStreamInfo
	Upstream() <-chan *Message 
}

type NetStreamDownstream interface {
	Info() *NetStreamInfo
	Downstream() chan<- *Message 
}

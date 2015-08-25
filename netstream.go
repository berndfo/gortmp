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

func RegisterNewNetStream(name string, streamType string, inboundConn *Conn) (info *NetStreamInfo, err error) {
	if _, exists := netStreams[name]; exists {
		return nil, ErrorNameAlreadyExists
	}
	
	info = &NetStreamInfo{
		Name: name,
		Type: streamType,
		Conn: inboundConn,
	}

	ns := netStream {
		info: info,
		downstreams: make([]*NetStreamDownstream, 0),
	}
	
	_ = ns
	
	return
}

type NetStreamInfo struct {
	Name string
	Type string // "live", "record" or "append"
	Conn *Conn
}

type NetStreamUpstream interface {
	Info() *NetStreamInfo
	Upstream() <-chan *Message 
}

type NetStreamDownstream interface {
	Info() *NetStreamInfo
	Downstream() chan<- *Message 
}

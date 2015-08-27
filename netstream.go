package gortmp
import (
	"errors"
	"log"
)

type NetStreamInfo struct {
	Name string
	Type string // "live", "record" or "append"
	Stream ServerStream
}

type NetStreamUpstream interface {
	Info() NetStreamInfo
	Upstream() <-chan *Message 
}

type NetStreamDownstream interface {
	Info() NetStreamInfo
	PushDownstream(*Message)
}

var netStreams map[string]*netStream = make(map[string]*netStream)

type netStream struct {
	info NetStreamInfo
	upstream NetStreamUpstream
	downstreams []NetStreamDownstream
}

// implements NetStreamUpstream
type netUpstream struct {
	info NetStreamInfo
	upstreamChan <-chan *Message 
}
func (ns *netUpstream) Info() NetStreamInfo {
	return ns.info
}
func (ns *netUpstream) Upstream() <-chan *Message {
	return ns.upstreamChan
}

var ErrorNameAlreadyExists error = errors.New("NameAlreadyExists") 
var StreamNotExists error = errors.New("StreamNotExists") 

func RegisterNewNetStream(name string, streamType string, serverStream ServerStream) (upstream NetStreamUpstream, dispatcher *NetStreamDispatchingHandler, err error) {
	if _, exists := netStreams[name]; exists {
		return nil, &NetStreamDispatchingHandler{}, ErrorNameAlreadyExists
	}
	
	msgChan := make(chan *Message)
	
	info := NetStreamInfo{
		Name: name,
		Type: streamType,
		Stream: serverStream,
	}

	ns := netStream {
		info: info,
		downstreams: make([]NetStreamDownstream, 0),
	}
	netStreams[name] = &ns
	
	go func() {
		for {
			select {
				case msg := <-msgChan:
					if msg == nil {
						return
					}
					for _, downstream := range ns.downstreams {
						downstream.PushDownstream(msg)
					}
			}
		}
	}()
	
	dispatcherHandler := NetStreamDispatchingHandler{msgChan}
	
	upstream = &netUpstream{info: info, upstreamChan: msgChan}
	
	return upstream, &dispatcherHandler, nil
}

func RegisterDownstream(name string, downstream NetStreamDownstream) error {
	netstream, exists := netStreams[name]
	if (!exists) {
		return StreamNotExists
	}
	netstream.downstreams = append(netstream.downstreams, downstream)
	return nil
}


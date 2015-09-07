package gortmp
import (
	"errors"
	"sync/atomic"
	"log"
	"time"
	"sync"
)

const UPSTREAM_DEFAULT_CHANNEL_SIZE int = 10000

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
	PushDownstream(*Message) error
}

var netStreams map[string]*netStream = make(map[string]*netStream)
var netStreamsLock sync.Mutex 

type netStream struct {
	info NetStreamInfo
	upstream NetStreamUpstream
	downstreamsLock sync.Mutex
	downstreams []NetStreamDownstream
}

// implements NetStreamUpstream
type netUpstream struct {
	info NetStreamInfo
	upstreamChan <-chan *Message
	messagesReceived uint64
}
func (ns *netUpstream) Info() NetStreamInfo {
	return ns.info
}
func (ns *netUpstream) Upstream() <-chan *Message {
	return ns.upstreamChan
}

var ErrorNameAlreadyExists error = errors.New("NameAlreadyExists") 
var StreamNotExists error = errors.New("StreamNotExists") 
var DownstreamClosed error = errors.New("DownstreamClosed") 

func RegisterNewNetStream(name string, streamType string, serverStream ServerStream) (upstream NetStreamUpstream, dispatcher *NetStreamDispatchingHandler, err error) {
	netStreamsLock.Lock()
	defer netStreamsLock.Unlock()
	
	if _, exists := netStreams[name]; exists {
		return nil, &NetStreamDispatchingHandler{}, ErrorNameAlreadyExists
	}
	
	msgChan := make(chan *Message, UPSTREAM_DEFAULT_CHANNEL_SIZE)
	
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

	netupstream := &netUpstream{info: info, upstreamChan: msgChan}
	
	quitChan := make(chan error)
	
	// print statistics loop 
	go func() {
		for {
			select {
				case <-quitChan:
					return;
				case <-time.After(5*time.Second):
					log.Printf("upstream relay for %q relayed %d msg to %d downstreams", name, netupstream.messagesReceived, len(ns.downstreams))
			}
		}
	} ()
	
	// message loop
	go func() {
		defer close(quitChan)

		var throughput = 0
		printThroughputDuration := 1*time.Second
		printThroughput := time.After(printThroughputDuration)
		for {
			select {
				case msg := <-msgChan:
					if msg == nil {
						return
					}
					atomic.AddUint64(&netupstream.messagesReceived, 1)
					throughput++
					//log.Printf("relaying msg to %d downstreams: %s", len(ns.downstreams), msg.Dump(""))
					ns.downstreamsLock.Lock()
					
					for _, downstream := range ns.downstreams {
						err := downstream.PushDownstream(msg)
						if err == DownstreamClosed {
							log.Printf("closed downstream still needs to be removed from relay list (call UnregisterDownstream()).")
						}
					}
					ns.downstreamsLock.Unlock()
				case <-printThroughput:
					log.Printf("[%s] relayed msg throughput %d/%s", name, throughput, printThroughputDuration)
					throughput = 0
					printThroughput = time.After(printThroughputDuration)
			}
		}
	}()
	
	upstream = netupstream
	
	dispatcherHandler := NetStreamDispatchingHandler{msgChan}
	
	return upstream, &dispatcherHandler, nil
}

func FindNetStream(name string) (info NetStreamInfo, exists bool) {
	var netstream *netStream
	if netstream, exists = netStreams[name]; !exists {
		return NetStreamInfo{}, false
	}
	return netstream.info, true 
}

func RegisterDownstream(name string, downstream NetStreamDownstream) error {
	netStreamsLock.Lock()
	defer netStreamsLock.Unlock()
	
	netstream, exists := netStreams[name]
	if (!exists) {
		return StreamNotExists
	}
	netstream.downstreamsLock.Lock()
	defer netstream.downstreamsLock.Unlock()
	netstream.downstreams = append(netstream.downstreams, downstream)
	return nil
}

func UnregisterDownstream(name string, removedDownstream NetStreamDownstream) error {
	netStreamsLock.Lock()
	defer netStreamsLock.Unlock()
	
	netstream, exists := netStreams[name]
	if (!exists) {
		return StreamNotExists
	}
	
	netstream.downstreamsLock.Lock()
	defer netstream.downstreamsLock.Unlock()
	
	leftDownstreams := make([]NetStreamDownstream, 0)
	for _, downstream := range netstream.downstreams {
		if downstream != removedDownstream {
			leftDownstreams = append(leftDownstreams, downstream) // keep if not to be removed
		}
	}
	netstream.downstreams = leftDownstreams
	
	return nil
}


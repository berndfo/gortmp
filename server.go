// Copyright 2013, zhangpeihao All rights reserved.
package gortmp

import (
	"bufio"
	"log"
	"net"
	"time"
	"expvar"
)

type ServerHandler interface {
	NewConnection(conn ServerConn, connectReq *Command, server *Server) bool
}

type Server struct {
	listener    net.Listener
	network     string
	bindAddress string
	exit        bool
	handler     ServerHandler
}

// Create a new server.
func NewServer(network string, bindAddress string, handler ServerHandler) (*Server, error) {
	server := &Server{
		network:     network,
		bindAddress: bindAddress,
		exit:        false,
		handler:     handler,
	}
	var err error
	server.listener, err = net.Listen(server.network, server.bindAddress)
	if err != nil {
		return nil, err
	}
	
	serverConnEstablishedChan := make(chan ServerConn, 50)
	serverConnLostChan := make(chan ServerConn, 50)
	go func() {
		// collect and report all connections
		pVar := expvar.NewInt("client.connections.count")
		allCons := make([]ServerConn, 0)
		for {
			select {
				case newServerConn := <-serverConnEstablishedChan:
					if (newServerConn == nil) {
						return
					}
					allCons = append(allCons, newServerConn)
					pVar.Set(int64(len(allCons)))
				case serverConnLost := <-serverConnLostChan:
					newCons := make([]ServerConn, 0)
					for _, conn := range allCons {
						if (conn == serverConnLost) {
							log.Println("removed 1 lost conn")
							continue
						}
						newCons = append(newCons, conn)
					}
					allCons = newCons
					pVar.Set(int64(len(allCons)))
				case <-time.After(time.Duration(1*time.Minute)):
					log.Printf("current connection count: %d", len(allCons))
			}
		}
	}()
	
	log.Printf("Start listening on %q...",server.listener.Addr().String())
	go server.acceptConnectionLoop(serverConnEstablishedChan, serverConnLostChan)
	return server, nil
}

// Close listener.
func (server *Server) Close() {
	log.Println("Stop server")
	server.exit = true
	server.listener.Close()
}

func (server *Server) acceptConnectionLoop(serverConnEstablishedChan chan<- ServerConn, serverConnLostChan chan<- ServerConn) {
	for !server.exit {
		c, err := server.listener.Accept()
		if err != nil {
			if server.exit {
				break
			}
			log.Println("SocketServer listener error:", err)
			server.rebind()
		}
		if c != nil {
			go server.Handshake(c, serverConnEstablishedChan, serverConnLostChan)
		}
	}
}

func (server *Server) rebind() {
	listener, err := net.Listen(server.network, server.bindAddress)
	if err == nil {
		server.listener = listener
	} else {
		time.Sleep(time.Second)
	}
}

func (server *Server) Handshake(c net.Conn, serverConnEstablishedChan chan<- ServerConn, serverConnLostChan chan<- ServerConn) {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			log.Println("Server::Handshake panic error:", err)
		}
	}()

	log.Printf("[%s]SHandshake beginning with client", c.RemoteAddr().String())
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	timeout := time.Duration(10) * time.Second
	if err := SHandshake(c, br, bw, timeout); err != nil {
		log.Printf("[%s]SHandshake error: %s", c.RemoteAddr().String(), err.Error())
		c.Close()
		return
	}
	// New client connects to server
	serverConn, err := NewServerConn(c, br, bw, server, 100, serverConnLostChan)
	if err != nil {
		log.Printf("[%s]NewServerConn error: %s", c.RemoteAddr().String(), err.Error())
		c.Close()
		return
	}
	serverConnEstablishedChan <-serverConn
}

// On received connect request
func (server *Server) OnConnectAuth(conn ServerConn, connectReq *Command) bool {
	return server.handler.NewConnection(conn, connectReq, server)
}

// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"github.com/berndfo/goamf"
	"log"
	"fmt"
)

// Command
//
// Command messages carry the AMF encoded commands between the client
// and the server. A client or a server can request Remote Procedure
// Calls (RPC) over streams that are communicated using the command
// messages to the peer.
type Command struct {
	IsFlex        bool
	Name          string
	TransactionID uint32
	Objects       []interface{}
}

func (cmd *Command) Write(w Writer) (err error) {
	if cmd.IsFlex {
		err = w.WriteByte(0x00)
		if err != nil {
			return
		}
	}
	_, err = amf.WriteString(w, cmd.Name)
	if err != nil {
		return
	}
	_, err = amf.WriteDouble(w, float64(cmd.TransactionID))
	if err != nil {
		return
	}
	for _, object := range cmd.Objects {
		_, err = amf.WriteValue(w, object)
		if err != nil {
			return
		}
	}
	return
}

func (cmd *Command) ObjectLen() int {
	return len(cmd.Objects)
}

func (cmd *Command) Object(i int) (obj interface{}, exists bool) {
	if i < 0 || i >= len(cmd.Objects) {
		return nil, false
	}
	obj = cmd.Objects[i]
	return obj, obj != nil 
}

func (cmd *Command) ObjectString(i int) (str string, exists bool) {
	var obj interface{} 
	if obj, exists = cmd.Object(i); !exists {
		return "", false
	}
	str, ok := obj.(string)
	return str, ok   
}

func (cmd *Command) ObjectObject(i int) (amfObj amf.Object, exists bool) {
	var obj interface{} 
	if obj, exists = cmd.Object(i); !exists {
		return nil, false
	}
	amfObj, ok := obj.(amf.Object)
	return amfObj, ok   
}

func (cmd *Command) ObjectBool(i int) (flag bool, exists bool) {
	var obj interface{} 
	if obj, exists = cmd.Object(i); !exists {
		return false, false
	}
	flag, ok := obj.(bool)
	return flag, ok   
}

func (cmd *Command) ObjectNumber(i int) (number float64, exists bool) {
	var obj interface{} 
	if obj, exists = cmd.Object(i); !exists {
		return 0.0, false
	}
	number, ok := obj.(float64)
	return number, ok   
}

func (cmd *Command) LogDump(name string) {
	log.Println(cmd.Dump(name))
}
func (cmd *Command) Dump(name string) string {
	objs := cmd.Objects
	objDump := "nil"
	if objs != nil {
		objDump = "["
		for key, obj := range objs {
			switch obj.(type) {
			default:
				objDump += fmt.Sprintf("obj[%d]: %v (%T), ", key, obj, obj)
			case amf.Object:
				objDump += fmt.Sprintf("obj[%d]: map[", key)
				objMap := obj.(amf.Object)
				for mapkey, mapvalue := range objMap {
					if mapkey == "audioCodecs" || mapkey == "videoCodecs" || mapkey == "capabilities" {
						fValue := mapvalue.(float64)
						objDump += fmt.Sprintf("%s: 0x%04x=%020b, ", mapkey, int(fValue), int(fValue))
					} else {
						objDump += fmt.Sprintf("%s: %v, ", mapkey, mapvalue)
					}
				}
				objDump += "], "
			}
		}
		objDump += "]"
	}
	return fmt.Sprintf("Command{IsFlex: %t, Name: %s, TransactionID: %d, Objects: %s}",
		cmd.IsFlex, cmd.Name, cmd.TransactionID, objDump)
}

# GoRTMP, revised edition
======
[ ![Codeship Status for berndfo/gortmp](https://codeship.com/projects/49aac150-343d-0133-e0dc-56c8db4126b8/status?branch=master)](https://codeship.com/projects/100453)

RTMP protocol implementation.

## Spec: 
* RTMP - http://www.adobe.com/devnet/rtmp.html
* AMF0 - http://download.macromedia.com/pub/labs/amf/amf0_spec_121207.pdf
* AMF3 - http://download.macromedia.com/pub/labs/amf/amf3_spec_121207.pdf

## Dependencies

depends solely on

* goflv - https://github.com/berndfo/goflv
* goamf - https://github.com/berndfo/goamf

## Running the server:

start the standalone server

```go run src/github.com/berndfo/gortmp/server/main/server.go```
 
the server listens on the default RTMP port 1935.

the server will accept client connections for either 'publish' or 'play' netstreams.
if the publish mode is 'record' or 'append' it will write the stream to FLV files in the local file system.

the server exposes runtime variables on localhost via the URL

```http://localhost:8000/debug/vars```

## Running a publishing client:

if you want to publish a FLV file named myvideo.flv to the server, run 
```go run src/github.com/berndfo/gortmp/demo/publisher/rtmp_publisher.go -FLV myvideo.flv -Stream myvideo```
 
optionally, specify the server URL by appending for example 
```-URL rtmp://localhost:1935/stream```



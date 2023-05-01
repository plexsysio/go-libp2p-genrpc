# go-libp2p-genrpc ping bidirectional stream example

Quick example how to build a bidirectional ping service with go-libp2p-genrpc

This example has two parts, the `host` and the `client`. You can switch between
them with the `-mode` flag that accepts either `host` or `client` as value.

## Usage

Have two terminal windows open in the `examples/pingstream` directory. In the first
one, run:

```
$ go run pingstream.go -mode host
```

And then copy one of the "I'm listening on" addresses. In this example, we use
the `127.0.0.1` one which ends up being:

```
/ip4/127.0.0.1/tcp/9000/ipfs/QmTwhWUFdY8NvhmLxE9CzPm29zC9bzfoMGAz2SFV5cb26d
```

Now in the second terminal window, run:

```
$ go run pingstream.go -mode client -host /ip4/127.0.0.1/tcp/9000/ipfs/QmTwhWUFdY8NvhmLxE9CzPm29zC9bzfoMGAz2SFV5cb26d
```

And you should start seeing log messages showing the duration of each ping, and
finally a average of 10 pings.

```
2018/06/10 12:52:44 Launching client
2018/06/10 12:52:44 Hello World, my hosts ID is Qmapkii8GMB2fMUT66yds9surJUdsZHMtygFSFhPnHa14K
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=1 time=1.404259ms
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=2 time=1.338412ms
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=3 time=892.567µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=4 time=505.573µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=5 time=565.036µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=6 time=765.652µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=7 time=1.296701ms
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=8 time=804.552µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=9 time=733.054µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=10 time=688.807µs
Average duration for ping reply: 899.461µs
```

## Explanation

Here is some of the important code snippets from this example. Keep in mind
that some information here is hard-coded and error-handling is omitted for brevity
and is not a example of production-ready code. To see a more real version of
this code, please check the `pingstream.go` file inside this directory.

### Host

First we create our libp2p host:

```golang
host, _ := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/9000"))
```

After that, we create our genrpc mux that will be used to register the services

```golang
mux := genrpc.New("PingService")
```

The Ping birectional stream API needs to follow the BidirStream handler format.
We can either define a new struct for the server and write the methods on it, or
we can also directly register the functions on the paths.

For this eg, we have used the first approach. We define the request and response
types and the object implementing the service.

```golang
type PingReq struct {
	Start time.Time
	Data  []byte
}

type PingReply struct {
	Start time.Time
	Data  []byte
}
type PingService struct{}

func (t *PingService) ping(ctx context.Context, req <-chan *PingReq) (<-chan *PingReply, error) {
	respC := make(chan *PingReply)
	go func() {
		defer close(respC)
		for req := range req {
			reply := new(PingReply)
			reply.Data = req.Data
			reply.Start = req.Start
			select {
			case <-ctx.Done():
				return
			case respC <- reply:
			}
		}
	}()
	return respC, nil
}
```

Once we have those defined, we can register our PingService with the Mux

```golang
mux.Handle("Ping", genrpc.BidirStream(pingSvc.ping))
```

Now our host is ready to reply to pings from the client.

### Client

Again, let's first create our libp2p peer

```golang
client, _ := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/9001"))
```

Now we need to first connect the client and server hosts. This needs to be done by the
user in actual production code. The library does not care about the underlying transport
as such. All it needs is a peer.ID to send messages provided the peer is connected.

```golang
host := "/ip4/127.0.0.1/tcp/9000/ipfs/QmUGZS556mhYSSrFGJpxtt33QQuRDjhsYvFNCW1V3A4wjL"
ma, _ := multiaddr.NewMultiaddr(host)
peerInfo, _ := peerstore.InfoFromP2pAddr(ma)
ctx := context.Background()
client.Connect(ctx, *peerInfo)
```

And now we can create our new request, the request and response types should be the same

```golang
req := genrpc.NewBidirStreamReq[PingReq, PingReply](client, "PingService", "Ping")
```

To make sure that we actually make the call correct, we add some random data
in the arguments so we can check it when we get the reply.

```golang
c := 64
b := make([]byte, c)
rand.Read(b)
args.Data = b
```

Now we can finally make the call itself! Keep in mind this is a blocking call,
and it'll start sending the responses on the channel returned. The first message
reports any error so we need to start sending messages on the request channel before
calling the Execute.

```golang
  go func() {
		for numCalls < pingCount {
			var args PingReq
			c := randomDataSize
			b := make([]byte, c)
			_, err := rand.Read(b)
			if err != nil {
				panic(err)
			}

			args.Data = b
			args.Start = time.Now()
			reqC <- &args
			reqs[numCalls] = &args
			numCalls++
			time.Sleep(betweenPingsSleep)
		}
		close(reqC)
	}()

	replyC, err := req.Execute(ctx, peerInfo.ID, reqC)
	if err != nil {
		panic(err)
	}
```

Once the call above has finished, `reply.Data` should now have the same data
as we had before

```golang
if bytes.Equal(reply.Data, b) {
  fmt.Println("Got the same bytes back as we sent!")
}
```

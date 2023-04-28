package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"time"

	"log"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	genrpc "github.com/plexsysio/go-libp2p-genrpc"

	multiaddr "github.com/multiformats/go-multiaddr"
)

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

func createPeer(listenAddr string) host.Host {
	// Create a new libp2p host
	h, err := libp2p.New(libp2p.ListenAddrStrings(listenAddr))
	if err != nil {
		panic(err)
	}
	return h
}

func startServer() {
	log.Println("Launching host")
	host := createPeer("/ip4/0.0.0.0/tcp/9000")

	log.Printf("Hello World, my hosts ID is %s\n", host.ID().Pretty())
	for _, addr := range host.Addrs() {
		ipfsAddr, err := multiaddr.NewMultiaddr("/ipfs/" + host.ID().Pretty())
		if err != nil {
			panic(err)
		}
		peerAddr := addr.Encapsulate(ipfsAddr)
		log.Printf("I'm listening on %s\n", peerAddr)
	}

	pingSvc := &PingService{}
	mux := genrpc.New("PingService")
	mux.Handle("Ping", genrpc.BidirStream(pingSvc.ping))

	genrpc.Register(host, mux)

	fmt.Println("Done")

	for {
		time.Sleep(time.Second * 1)
	}
}

func startClient(host string, pingCount, randomDataSize int) {
	fmt.Println("Launching client")
	client := createPeer("/ip4/0.0.0.0/tcp/9001")
	fmt.Printf("Hello World, my hosts ID is %s\n", client.ID().Pretty())
	ma, err := multiaddr.NewMultiaddr(host)
	if err != nil {
		panic(err)
	}
	peerInfo, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	err = client.Connect(ctx, *peerInfo)
	if err != nil {
		panic(err)
	}

	req := genrpc.NewBidirStreamReq[PingReq, PingReply](client, "PingService", "Ping")
	numCalls := 0
	durations := []time.Duration{}
	betweenPingsSleep := time.Second * 1

	reqC := make(chan *PingReq)
	reqs := make([]*PingReq, pingCount)

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

	i := 0
	for reply := range replyC {
		if !bytes.Equal(reqs[i].Data, reply.Data) {
			panic("Received wrong amount of bytes back!")
		}
		diff := time.Now().Sub(reqs[i].Start)
		fmt.Printf("%d bytes from %s (%s): seq=%d time=%s\n", len(reply.Data), peerInfo.ID.String(), peerInfo.Addrs[0].String(), i+1, diff)
		i++
		durations = append(durations, diff)
	}

	totalDuration := int64(0)
	for _, d := range durations {
		totalDuration += d.Nanoseconds()
	}
	avgDuration := time.Duration(totalDuration / int64(len(durations)))
	fmt.Printf("Average duration for ping reply: %s\n", time.Duration(avgDuration))
}

func main() {

	var mode string
	var host string
	var count int
	var size int
	flag.StringVar(&mode, "mode", "", "host or client mode")
	flag.StringVar(&host, "host", "", "address of host to connect to")
	flag.IntVar(&count, "count", 10, "number of pings to make")
	flag.IntVar(&size, "size", 64, "size of random data in ping message")
	flag.Parse()

	if mode == "" {
		log.Fatal("You need to specify '-mode' to be either 'host' or 'client'")
	}

	if mode == "host" {
		startServer()
		return
	}
	if mode == "client" {
		if host == "" {
			log.Fatal("You need to specify '-host' when running as a client")
		}
		startClient(host, count, size)
		return
	}
	log.Fatal("Mode '" + mode + "' not recognized. It has to be either 'host' or 'client'")
}

package genrpc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	genrpc "github.com/plexsysio/go-libp2p-genrpc"
	pbv1 "github.com/plexsysio/go-libp2p-genrpc/gen/v1"
	"github.com/vmihailenco/msgpack/v5"
)

type testReq struct {
	Arg string
}

type testResp struct {
	Arg string
}

type ctxKey struct{}

func createClientServer(t *testing.T, handler network.StreamHandler) (*swarm.Swarm, *swarm.Swarm) {
	t.Helper()

	server := swarmt.GenSwarm(t)
	client := swarmt.GenSwarm(t)
	t.Cleanup(func() {
		if err := errors.Join(server.Close(), client.Close()); err != nil {
			t.Fatalf("failed closing swarm: %v", err)
		}
	})

	// add handler on server
	server.SetStreamHandler(handler)
	// add server addr for client
	client.Peerstore().AddAddrs(server.LocalPeer(), server.ListenAddresses(), time.Hour)

	return server, client
}

func TestUnaryHandler(t *testing.T) {
	t.Parallel()

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "testReq"}
		tResp := &testResp{Arg: "testResp"}

		unaryHdlr := genrpc.Unary(func(ctx context.Context, req *testReq) (*testResp, error) {
			if req.Arg != tReq.Arg {
				t.Fatalf("unexpected req: got %v want: %v", req, tReq)
			}
			return tResp, nil
		})

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, unaryHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		body, err := msgpack.Marshal(tReq)
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)
		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		resp := &testResp{}
		err = msgpack.Unmarshal(msg.Body, resp)
		if err != nil {
			t.Fatalf("failed unmarshaling message: %v", err)
		}

		if resp.Arg != "testResp" {
			t.Fatalf("incorrect response arg received: got %s want %s", resp.Arg, tResp.Arg)
		}
	})

	t.Run("return error", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "testReq"}
		tErr := errors.New("dummy error")

		unaryHdlr := genrpc.Unary(func(ctx context.Context, req *testReq) (*testResp, error) {
			if req.Arg != tReq.Arg {
				t.Fatalf("unexpected req: got %v want: %v", req, tReq)
			}
			return nil, tErr
		})

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, unaryHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		body, err := msgpack.Marshal(tReq)
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)
		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		if !strings.Contains(msg.Error, tErr.Error()) {
			t.Fatalf("unexpected error: got %s want %s", msg.Error, tErr.Error())
		}
	})

	t.Run("middleware", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "testReq"}
		tHdr := map[string][]byte{"key": []byte("value")}
		tResp := &testResp{Arg: "testResp"}

		unaryHdlr := genrpc.Unary(
			func(ctx context.Context, req *testReq) (*testResp, error) {
				if ctx.Value(ctxKey{}) != "value" {
					t.Fatal("expected middleware op")
				}
				if req.Arg != tReq.Arg {
					t.Fatalf("unexpected req: got %v want: %v", req, tReq)
				}
				return tResp, nil
			},
			func(next genrpc.UnaryHandlerFunc) genrpc.UnaryHandlerFunc {
				return func(ctx context.Context, hdrs map[string][]byte, req []byte) error {
					if !bytes.Equal(hdrs["key"], []byte("value")) {
						t.Fatalf("unexpected header: %v", hdrs)
					}
					return next(context.WithValue(ctx, ctxKey{}, "value"), hdrs, req)
				}
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, unaryHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		body, err := msgpack.Marshal(tReq)
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)
		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Headers: tHdr, Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		resp := &testResp{}
		err = msgpack.Unmarshal(msg.Body, resp)
		if err != nil {
			t.Fatalf("failed unmarshaling message: %v", err)
		}

		if resp.Arg != "testResp" {
			t.Fatalf("incorrect response arg received: got %s want %s", resp.Arg, tResp.Arg)
		}
	})

	t.Run("middleware error", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "testReq"}
		tErr := errors.New("dummy error")

		unaryHdlr := genrpc.Unary(
			func(ctx context.Context, req *testReq) (*testResp, error) {
				return &testResp{}, nil
			},
			func(next genrpc.UnaryHandlerFunc) genrpc.UnaryHandlerFunc {
				return func(ctx context.Context, hdrs map[string][]byte, req []byte) error {
					return tErr
				}
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, unaryHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		body, err := msgpack.Marshal(tReq)
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)
		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		if !strings.Contains(msg.Error, tErr.Error()) {
			t.Fatalf("unexpected error: got %s want %s", msg.Error, tErr.Error())
		}
	})
}

func TestUpStreamHandler(t *testing.T) {
	t.Parallel()

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		reqs := make([]*testReq, 10)
		for i := 0; i < 10; i++ {
			reqs[i] = &testReq{Arg: fmt.Sprintf("message #%d", i+1)}
		}
		tResp := &testResp{Arg: "Done"}

		upStreamHdlr := genrpc.UpStream(
			func(ctx context.Context, reqC <-chan *testReq) (*testResp, error) {
				i := 0
				for r := range reqC {
					if !strings.Contains(r.Arg, reqs[i].Arg) {
						t.Fatalf("incorrect msg received: got %s want %s", r.Arg, reqs[i].Arg)
					}
					i++
				}
				return tResp, nil
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, upStreamHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}
		wrappedStream := genrpc.WrapStream(s)

		for _, r := range reqs {
			body, err := msgpack.Marshal(r)
			if err != nil {
				t.Fatalf("unexpected error marshaling req: %v", err)
			}

			err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
			if err != nil {
				t.Fatalf("unexpected error writing req: %v", err)
			}
		}

		err = s.CloseWrite()
		if err != nil {
			t.Fatalf("unexpected error while CloseWrite: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		resp := &testResp{}
		err = msgpack.Unmarshal(msg.Body, resp)
		if err != nil {
			t.Fatalf("failed unmarshaling message: %v msg: %+v", err, msg)
		}

		if resp.Arg != tResp.Arg {
			t.Fatalf("incorrect response arg received: got %s want %s", resp.Arg, tResp.Arg)
		}
	})

	t.Run("return error", func(t *testing.T) {
		t.Parallel()

		reqs := make([]*testReq, 10)
		for i := 0; i < 10; i++ {
			reqs[i] = &testReq{Arg: fmt.Sprintf("message #%d", i+1)}
		}
		tErr := errors.New("dummy error")

		upStreamHdlr := genrpc.UpStream(
			func(ctx context.Context, reqC <-chan *testReq) (*testResp, error) {
				i := 0
				for r := range reqC {
					if !strings.Contains(r.Arg, reqs[i].Arg) {
						t.Fatalf("incorrect msg received: got %s want %s", r.Arg, reqs[i].Arg)
					}
					i++
					if i > 5 {
						return nil, tErr
					}
				}
				return &testResp{}, nil
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, upStreamHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}
		wrappedStream := genrpc.WrapStream(s)

		for _, r := range reqs {
			body, err := msgpack.Marshal(r)
			if err != nil {
				t.Fatalf("unexpected error marshaling req: %v", err)
			}

			err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
			if err != nil {
				t.Fatalf("unexpected error writing req: %v", err)
			}
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		if !strings.Contains(msg.Error, tErr.Error()) {
			t.Fatalf("unexpected error: got %s want %s", msg.Error, tErr.Error())
		}
	})

	t.Run("middleware", func(t *testing.T) {
		t.Parallel()

		reqs := make([]*testReq, 10)
		for i := 0; i < 10; i++ {
			reqs[i] = &testReq{Arg: fmt.Sprintf("message #%d", i+1)}
		}
		tHdr := map[string][]byte{"key": []byte("value")}
		tResp := &testResp{Arg: "Done"}

		upStreamHdlr := genrpc.UpStream(
			func(ctx context.Context, reqC <-chan *testReq) (*testResp, error) {
				if ctx.Value(ctxKey{}) != "value" {
					t.Fatal("expected middleware op")
				}
				i := 0
				for r := range reqC {
					if !strings.Contains(r.Arg, reqs[i].Arg) {
						t.Fatalf("incorrect msg received: got %s want %s", r.Arg, reqs[i].Arg)
					}
					i++
				}
				return tResp, nil
			},
			func(next genrpc.StreamHandlerFunc) genrpc.StreamHandlerFunc {
				return func(ctx context.Context, hdrs map[string][]byte, req <-chan []byte) error {
					if !bytes.Equal(hdrs["key"], []byte("value")) {
						t.Fatalf("unexpected header: %v", hdrs)
					}
					return next(context.WithValue(ctx, ctxKey{}, "value"), hdrs, req)
				}
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, upStreamHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}
		wrappedStream := genrpc.WrapStream(s)

		for _, r := range reqs {
			body, err := msgpack.Marshal(r)
			if err != nil {
				t.Fatalf("unexpected error marshaling req: %v", err)
			}

			err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Headers: tHdr, Body: body})
			if err != nil {
				t.Fatalf("unexpected error writing req: %v", err)
			}
		}

		err = s.CloseWrite()
		if err != nil {
			t.Fatalf("unexpected error while CloseWrite: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		resp := &testResp{}
		err = msgpack.Unmarshal(msg.Body, resp)
		if err != nil {
			t.Fatalf("failed unmarshaling message: %v", err)
		}

		if resp.Arg != tResp.Arg {
			t.Fatalf("incorrect response arg received: got %s want %s", resp.Arg, tResp.Arg)
		}
	})

	t.Run("middleware error", func(t *testing.T) {
		t.Parallel()

		tErr := errors.New("dummy error")

		upStreamHdlr := genrpc.UpStream(
			func(ctx context.Context, reqC <-chan *testReq) (*testResp, error) {
				t.Fatal("should not be called")
				return nil, nil
			},
			func(next genrpc.StreamHandlerFunc) genrpc.StreamHandlerFunc {
				return func(ctx context.Context, hdrs map[string][]byte, req <-chan []byte) error {
					return tErr
				}
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, upStreamHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}
		wrappedStream := genrpc.WrapStream(s)

		gotErr := make(chan struct{})
		go func() {
			defer close(gotErr)

			msg, err := wrappedStream.ReadMsg(ctx)
			if err != nil {
				t.Fatalf("unexpected error while reading message: %v", err)
			}

			if !strings.Contains(msg.Error, tErr.Error()) {
				t.Fatalf("unexpected error: got %s want %s", msg.Error, tErr.Error())
			}
		}()

	LOOP:
		for {
			select {
			case <-gotErr:
				break LOOP
			default:
			}

			body, err := msgpack.Marshal(&testReq{Arg: "dummy"})
			if err != nil {
				t.Fatalf("unexpected error marshaling req: %v", err)
			}

			err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
			if err != nil {
				select {
				case <-gotErr:
					break LOOP
				case <-time.After(1 * time.Second):
					t.Fatalf("unexpected error writing req: %v", err)
				}
			}

			time.Sleep(5 * time.Millisecond)
		}
	})
}

func TestDownStreamHandler(t *testing.T) {
	t.Parallel()

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "test"}
		resps := make([]*testResp, 10)
		for i := 0; i < 10; i++ {
			resps[i] = &testResp{Arg: fmt.Sprintf("message #%d", i+1)}
		}

		downStreamHdlr := genrpc.DownStream(
			func(ctx context.Context, req *testReq) (<-chan *testResp, error) {
				if req.Arg != tReq.Arg {
					t.Fatalf("incorrect msg received: got %s want %s", req.Arg, tReq.Arg)
				}
				respC := make(chan *testResp)
				go func() {
					defer close(respC)
					for _, r := range resps {
						respC <- r
					}
				}()
				return respC, nil
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, downStreamHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}
		wrappedStream := genrpc.WrapStream(s)

		body, err := msgpack.Marshal(tReq)
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		i := 0
		for {
			msg, err := wrappedStream.ReadMsg(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error while reading message: %v", err)
			}

			if msg.Error != "" {
				t.Fatalf("unexpected error: %s", msg.Error)
			}

			if len(msg.Body) == 0 {
				break
			}

			resp := &testResp{}
			err = msgpack.Unmarshal(msg.Body, resp)
			if err != nil {
				t.Fatalf("failed unmarshaling message: %v", err)
			}

			if !reflect.DeepEqual(resp, resps[i]) {
				t.Fatalf("incorrect response arg received: got %s want %s", resp.Arg, resps[i].Arg)
			}
			i++
		}

		if len(resps) != i {
			t.Fatalf("expected all responses to be read")
		}
	})

	t.Run("return error", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "test"}
		tErr := errors.New("dummy error")

		downStreamHdlr := genrpc.DownStream(
			func(ctx context.Context, req *testReq) (<-chan *testResp, error) {
				if req.Arg != tReq.Arg {
					t.Fatalf("incorrect msg received: got %s want %s", req.Arg, tReq.Arg)
				}
				return nil, tErr
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, downStreamHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)

		body, err := msgpack.Marshal(tReq)
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		if !strings.Contains(msg.Error, tErr.Error()) {
			t.Fatalf("unexpected error: got %s want %s", msg.Error, tErr.Error())
		}
	})

	t.Run("middleware", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "test"}
		resps := make([]*testResp, 10)
		tHdr := map[string][]byte{"key": []byte("value")}
		mtx := sync.Mutex{}

		for i := 0; i < 10; i++ {
			resps[i] = &testResp{Arg: fmt.Sprintf("message #%d", i+1)}
		}

		downStreamHdlr := genrpc.DownStream(
			func(ctx context.Context, req *testReq) (<-chan *testResp, error) {
				if req.Arg != tReq.Arg {
					t.Fatalf("incorrect msg received: got %s want %s", req.Arg, tReq.Arg)
				}
				respC := make(chan *testResp)
				go func() {
					defer close(respC)
					for _, r := range resps {
						mtx.Lock()
						r.Arg += ctx.Value(ctxKey{}).(string)
						mtx.Unlock()
						respC <- r
					}
				}()
				return respC, nil
			},
			func(next genrpc.UnaryHandlerFunc) genrpc.UnaryHandlerFunc {
				return func(ctx context.Context, hdrs map[string][]byte, req []byte) error {
					if !reflect.DeepEqual(hdrs, tHdr) {
						t.Fatalf("unexpected header: %v", hdrs)
					}
					return next(context.WithValue(ctx, ctxKey{}, "value"), hdrs, req)
				}
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, downStreamHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)

		body, err := msgpack.Marshal(tReq)
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Headers: tHdr, Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		i := 0
		for {
			msg, err := wrappedStream.ReadMsg(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error while reading message: %v", err)
			}

			if msg.Error != "" {
				t.Fatalf("unexpected error: %s", msg.Error)
			}

			if len(msg.Body) == 0 {
				break
			}

			resp := &testResp{}
			err = msgpack.Unmarshal(msg.Body, resp)
			if err != nil {
				t.Fatalf("failed unmarshaling message: %v", err)
			}

			mtx.Lock()
			want := resps[i]
			mtx.Unlock()
			if !reflect.DeepEqual(resp, want) {
				t.Fatalf("incorrect response arg received: got %s want %s", resp.Arg, want.Arg)
			}
			i++
		}
	})

	t.Run("middleware error", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "test"}
		tErr := errors.New("dummy error")

		downStreamHdlr := genrpc.DownStream(
			func(ctx context.Context, req *testReq) (<-chan *testResp, error) {
				if req.Arg != tReq.Arg {
					t.Fatalf("incorrect msg received: got %s want %s", req.Arg, tReq.Arg)
				}
				return nil, tErr
			},
			func(next genrpc.UnaryHandlerFunc) genrpc.UnaryHandlerFunc {
				return func(ctx context.Context, hdrs map[string][]byte, req []byte) error {
					return tErr
				}
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, downStreamHdlr)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)

		body, err := msgpack.Marshal(tReq)
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		if !strings.Contains(msg.Error, tErr.Error()) {
			t.Fatalf("unexpected error: got %s want %s", msg.Error, tErr.Error())
		}
	})
}

func TestBidirStreamHandler(t *testing.T) {
	t.Parallel()

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		reqs := make([]*testReq, 10)
		resps := make([]*testResp, 10)

		for i := 0; i < 10; i++ {
			reqs[i] = &testReq{Arg: fmt.Sprintf("req message #%d", i+1)}
			resps[i] = &testResp{Arg: fmt.Sprintf("resp message #%d", i+1)}
		}

		bidirStreamHandler := genrpc.BidirStream(
			func(ctx context.Context, reqC <-chan *testReq) (<-chan *testResp, error) {
				respC := make(chan *testResp)
				go func() {
					defer close(respC)
					i := 0
					for req := range reqC {
						if req.Arg != reqs[i].Arg {
							t.Fatalf("incorrect msg received: got %s want %s", req.Arg, reqs[i].Arg)
						}
						resp := &testResp{Arg: resps[i].Arg}
						respC <- resp
						i++
					}
				}()
				return respC, nil
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, bidirStreamHandler)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)

		done := make(chan struct{})
		go func() {
			defer close(done)

			i := 0
			for {
				msg, err := wrappedStream.ReadMsg(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("unexpected error while reading message: %v", err)
				}

				if msg.Error != "" {
					t.Fatalf("unexpected error: %s", msg.Error)
				}

				if len(msg.Body) == 0 {
					break
				}

				resp := &testResp{}
				err = msgpack.Unmarshal(msg.Body, resp)
				if err != nil {
					t.Fatalf("failed unmarshaling message: %v", err)
				}

				if !reflect.DeepEqual(resp, resps[i]) {
					t.Fatalf("incorrect response arg received: got %s want %s", resp.Arg, resps[i])
				}
				i++
			}
		}()

		for _, req := range reqs {
			body, err := msgpack.Marshal(req)
			if err != nil {
				t.Fatalf("unexpected error marshaling req: %v", err)
			}

			err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
			if err != nil {
				t.Fatalf("unexpected error writing req: %v", err)
			}
		}
		s.CloseWrite()
		<-done
	})

	t.Run("return error", func(t *testing.T) {
		t.Parallel()

		tErr := errors.New("dummy error")

		bidirStreamHandler := genrpc.BidirStream(
			func(ctx context.Context, reqC <-chan *testReq) (<-chan *testResp, error) {
				return nil, tErr
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, bidirStreamHandler)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)

		body, err := msgpack.Marshal(&testReq{Arg: "req message"})
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		if !strings.Contains(msg.Error, tErr.Error()) {
			t.Fatalf("unexpected error: got %s want %s", msg.Error, tErr.Error())
		}
	})

	t.Run("middleware", func(t *testing.T) {
		t.Parallel()

		reqs := make([]*testReq, 10)
		tHdr := map[string][]byte{"test": []byte("test")}
		resps := make([]*testResp, 10)

		for i := 0; i < 10; i++ {
			reqs[i] = &testReq{Arg: fmt.Sprintf("req message #%d", i+1)}
			resps[i] = &testResp{Arg: fmt.Sprintf("resp message #%d", i+1)}
		}

		bidirStreamHandler := genrpc.BidirStream(
			func(ctx context.Context, reqC <-chan *testReq) (<-chan *testResp, error) {
				if ctx.Value(ctxKey{}) != "test" {
					t.Fatal("middleware not working")
				}
				respC := make(chan *testResp)
				go func() {
					defer close(respC)
					i := 0
					for req := range reqC {
						if req.Arg != reqs[i].Arg {
							t.Fatalf("incorrect msg received: got %s want %s", req.Arg, reqs[i].Arg)
						}
						resp := &testResp{Arg: resps[i].Arg}
						respC <- resp
						i++
					}
				}()
				return respC, nil
			},
			func(next genrpc.StreamHandlerFunc) genrpc.StreamHandlerFunc {
				return func(ctx context.Context, hdrs map[string][]byte, reqC <-chan []byte) error {
					if !strings.Contains(string(hdrs["test"]), "test") {
						t.Fatal("middleware not working")
					}
					ctx = context.WithValue(ctx, ctxKey{}, "test")
					return next(ctx, hdrs, reqC)
				}
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, bidirStreamHandler)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)

		done := make(chan struct{})
		go func() {
			defer close(done)

			i := 0
			for {
				msg, err := wrappedStream.ReadMsg(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("unexpected error while reading message: %v", err)
				}

				if msg.Error != "" {
					t.Fatalf("unexpected error: %s", msg.Error)
				}

				if len(msg.Body) == 0 {
					break
				}

				resp := &testResp{}
				err = msgpack.Unmarshal(msg.Body, resp)
				if err != nil {
					t.Fatalf("failed unmarshaling message: %v", err)
				}

				if !reflect.DeepEqual(resp, resps[i]) {
					t.Fatalf("incorrect response arg received: got %s want %s", resp.Arg, resps[i])
				}
				i++
			}
		}()

		for _, req := range reqs {
			body, err := msgpack.Marshal(req)
			if err != nil {
				t.Fatalf("unexpected error marshaling req: %v", err)
			}

			err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body, Headers: tHdr})
			if err != nil {
				t.Fatalf("unexpected error writing req: %v", err)
			}
		}

		s.CloseWrite()
		<-done
	})

	t.Run("middleware error", func(t *testing.T) {
		t.Parallel()

		tErr := errors.New("dummy error")

		bidirStreamHandler := genrpc.BidirStream(
			func(ctx context.Context, reqC <-chan *testReq) (<-chan *testResp, error) {
				return nil, nil
			},
			func(next genrpc.StreamHandlerFunc) genrpc.StreamHandlerFunc {
				return func(ctx context.Context, hdrs map[string][]byte, reqC <-chan []byte) error {
					return tErr
				}
			},
		)

		ctx := context.Background()

		sw1, sw2 := createClientServer(t, bidirStreamHandler)

		s, err := sw2.NewStream(ctx, sw1.LocalPeer())
		if err != nil {
			t.Fatalf("unexpected error creating NewStream: %v", err)
		}

		wrappedStream := genrpc.WrapStream(s)

		body, err := msgpack.Marshal(&testReq{Arg: "req message"})
		if err != nil {
			t.Fatalf("unexpected error marshaling req: %v", err)
		}

		err = wrappedStream.WriteMsg(ctx, &pbv1.Message{Body: body})
		if err != nil {
			t.Fatalf("unexpected error writing req: %v", err)
		}

		msg, err := wrappedStream.ReadMsg(ctx)
		if err != nil {
			t.Fatalf("unexpected error while reading message: %v", err)
		}

		if !strings.Contains(msg.Error, tErr.Error()) {
			t.Fatalf("unexpected error: got %s want %s", msg.Error, tErr.Error())
		}
	})
}

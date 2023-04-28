package genrpc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	genrpc "github.com/plexsysio/go-libp2p-genrpc"
	pbv1 "github.com/plexsysio/go-libp2p-genrpc/gen/v1"
	"github.com/vmihailenco/msgpack/v5"
)

func newTestStreamer(t *testing.T, sw *swarm.Swarm, path string) genrpc.Streamer {
	t.Helper()

	return genrpc.StreamerFunc(
		func(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
			t.Helper()
			if string(pids[0]) != path {
				t.Fatalf("unexpected protocol: got %s, want %s", pids[0], path)
			}
			return sw.NewStream(ctx, p)
		},
	)
}

func TestUnaryReq(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		s1, s2 := createClientServer(t, func(s network.Stream) {
			defer s.Close()

			ws := genrpc.WrapStream(s)

			reqMsg, err := ws.ReadMsg(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(string(reqMsg.Headers["test"]), "test") {
				t.Fatalf("unexpected headers: got %s, want %s", reqMsg.Headers["test"], "test")
			}

			req := new(testReq)
			err = msgpack.Unmarshal(reqMsg.Body, req)
			if err != nil {
				t.Fatal(err)
			}

			if req.Arg != "test" {
				t.Fatalf("unexpected request: got %s, want %s", req.Arg, "test")
			}

			respBody, err := msgpack.Marshal(&testResp{Arg: "test"})
			if err != nil {
				t.Fatal(err)
			}

			err = ws.WriteMsg(context.Background(), &pbv1.Message{Body: respBody})
			if err != nil {
				t.Fatal(err)
			}
		})

		req := genrpc.NewUnaryReq[testReq, testResp](newTestStreamer(t, s2, "test/0.0.0/test"), "test", "test")
		req.SetHeader("test", []byte("test"))

		resp, err := req.Execute(context.Background(), s1.LocalPeer(), &testReq{Arg: "test"})
		if err != nil {
			t.Fatal(err)
		}

		if resp.Arg != "test" {
			t.Fatalf("unexpected response got %s, want %s", resp.Arg, "test")
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		s1, s2 := createClientServer(t, func(s network.Stream) {
			defer s.Close()

			ws := genrpc.WrapStream(s)
			ctx := context.Background()

			reqMsg, err := ws.ReadMsg(ctx)
			if err != nil {
				t.Fatal(err)
			}

			req := new(testReq)
			err = msgpack.Unmarshal(reqMsg.Body, req)
			if err != nil {
				t.Fatal(err)
			}

			if req.Arg != "test" {
				t.Fatalf("unexpected request: got %s, want %s", req.Arg, "test")
			}

			err = ws.WriteMsg(ctx, &pbv1.Message{Error: "test"})
			if err != nil {
				t.Fatal(err)
			}
		})

		req := genrpc.NewUnaryReq[testReq, testResp](newTestStreamer(t, s2, "test/1.0.0/test"), "test", "test", "1.0.0")

		_, err := req.Execute(context.Background(), s1.LocalPeer(), &testReq{Arg: "test"})
		if !strings.Contains(err.Error(), "test") {
			t.Fatal("unexpected error")
		}
	})
}

func TestUpStreamReq(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		reqs := make([]*testReq, 10)
		for i := range reqs {
			reqs[i] = &testReq{Arg: fmt.Sprintf("test%d", i)}
		}

		s1, s2 := createClientServer(t, func(s network.Stream) {
			defer s.Close()

			ws := genrpc.WrapStream(s)
			ctx := context.Background()

			i := 0
			for {
				reqMsg, err := ws.ReadMsg(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(reqMsg.Headers["test"]), "test") {
					t.Fatalf("unexpected headers: got %s, want %s", reqMsg.Headers["test"], "test")
				}

				req := new(testReq)
				err = msgpack.Unmarshal(reqMsg.Body, req)
				if err != nil {
					t.Fatal(err)
				}

				if req.Arg != reqs[i].Arg {
					t.Fatalf("unexpected request: got %s, want %s", req.Arg, reqs[i].Arg)
				}
				i++
			}

			respBody, err := msgpack.Marshal(&testResp{Arg: "test"})
			if err != nil {
				t.Fatal(err)
			}

			err = ws.WriteMsg(ctx, &pbv1.Message{Body: respBody})
			if err != nil {
				t.Fatal(err)
			}
		})

		req := genrpc.NewUpStreamReq[testReq, testResp](newTestStreamer(t, s2, "test/0.0.0/test"), "test", "test")
		req.SetHeader("test", []byte("test"))

		reqC := make(chan *testReq)
		go func() {
			for _, r := range reqs {
				reqC <- r
			}
			close(reqC)
		}()

		resp, err := req.Execute(context.Background(), s1.LocalPeer(), reqC)
		if err != nil {
			t.Fatal(err)
		}

		if resp.Arg != "test" {
			t.Fatalf("unexpected response got %s, want %s", resp.Arg, "test")
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		reqs := make([]*testReq, 10)
		for i := range reqs {
			reqs[i] = &testReq{Arg: fmt.Sprintf("test%d", i)}
		}

		s1, s2 := createClientServer(t, func(s network.Stream) {
			defer s.Close()

			ws := genrpc.WrapStream(s)
			ctx := context.Background()

			i := 0
			for {
				reqMsg, err := ws.ReadMsg(ctx)
				if err != nil {
					t.Fatal(err)
				}

				req := new(testReq)
				err = msgpack.Unmarshal(reqMsg.Body, req)
				if err != nil {
					t.Fatal(err)
				}

				if req.Arg != reqs[i].Arg {
					t.Fatalf("unexpected request: got %s, want %s", req.Arg, reqs[i].Arg)
				}

				if i == 5 {
					break
				}
				i++
			}

			err := ws.WriteMsg(ctx, &pbv1.Message{Error: "test"})
			if err != nil {
				t.Fatal(err)
			}
		})

		req := genrpc.NewUpStreamReq[testReq, testResp](newTestStreamer(t, s2, "test/1.0.0/test"), "test", "test", "1.0.0")

		reqC := make(chan *testReq)
		go func() {
			for _, r := range reqs {
				reqC <- r
			}
			close(reqC)
		}()

		_, err := req.Execute(context.Background(), s1.LocalPeer(), reqC)
		if !strings.Contains(err.Error(), "test") {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestDownStreamReq(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "test"}
		resps := make([]*testResp, 10)

		for i := range resps {
			resps[i] = &testResp{Arg: fmt.Sprintf("test%d", i)}
		}

		s1, s2 := createClientServer(t, func(s network.Stream) {
			defer s.Close()

			ws := genrpc.WrapStream(s)
			ctx := context.Background()

			reqMsg, err := ws.ReadMsg(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(string(reqMsg.Headers["test"]), "test") {
				t.Fatalf("unexpected headers: got %s, want %s", reqMsg.Headers["test"], "test")
			}

			req := new(testReq)

			err = msgpack.Unmarshal(reqMsg.Body, req)
			if err != nil {
				t.Fatal(err)
			}

			if req.Arg != tReq.Arg {
				t.Fatalf("unexpected request: got %s, want %s", req.Arg, tReq.Arg)
			}

			for _, r := range resps {
				respBody, err := msgpack.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}

				err = ws.WriteMsg(ctx, &pbv1.Message{Body: respBody})
				if err != nil {
					t.Fatal(err)
				}
			}
		})

		req := genrpc.NewDownStreamReq[testReq, testResp](newTestStreamer(t, s2, "test/0.0.0/test"), "test", "test")
		req.SetHeader("test", []byte("test"))

		respC, err := req.Execute(context.Background(), s1.LocalPeer(), tReq)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		for resp := range respC {
			if resp.Arg != resps[i].Arg {
				t.Fatalf("unexpected response: got %s, want %s", resp.Arg, resps[i].Arg)
			}
			i++
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "test"}
		tErr := errors.New("test")

		s1, s2 := createClientServer(t, func(s network.Stream) {
			defer s.Close()

			ws := genrpc.WrapStream(s)
			ctx := context.Background()

			reqMsg, err := ws.ReadMsg(ctx)
			if err != nil {
				t.Fatal(err)
			}

			req := new(testReq)

			err = msgpack.Unmarshal(reqMsg.Body, req)
			if err != nil {
				t.Fatal(err)
			}

			if req.Arg != tReq.Arg {
				t.Fatalf("unexpected request: got %s, want %s", req.Arg, tReq.Arg)
			}

			err = ws.WriteMsg(ctx, &pbv1.Message{Error: tErr.Error()})
			if err != nil {
				t.Fatal(err)
			}
		})

		req := genrpc.NewDownStreamReq[testReq, testResp](
			newTestStreamer(t, s2, "test/2.1.0/test"),
			"test",
			"test",
			"2.1.0",
		)

		_, err := req.Execute(context.Background(), s1.LocalPeer(), tReq)
		if !strings.Contains(err.Error(), tErr.Error()) {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestBidirStreamReq(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		reqs := make([]*testReq, 10)
		resps := make([]*testResp, 10)

		for i := 0; i < 10; i++ {
			reqs[i] = &testReq{Arg: fmt.Sprintf("test req %d", i)}
			resps[i] = &testResp{Arg: fmt.Sprintf("test resp %d", i)}
		}

		s1, s2 := createClientServer(t, func(s network.Stream) {
			defer s.Close()

			ws := genrpc.WrapStream(s)
			ctx := context.Background()

			i := 0
			for {
				reqMsg, err := ws.ReadMsg(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}

				if !strings.Contains(string(reqMsg.Headers["test"]), "test") {
					t.Fatalf("unexpected headers: got %s, want %s", reqMsg.Headers["test"], "test")
				}

				req := new(testReq)
				err = msgpack.Unmarshal(reqMsg.Body, req)
				if err != nil {
					t.Fatal(err)
				}

				if req.Arg != reqs[i].Arg {
					t.Fatalf("unexpected request: got %s, want %s", req.Arg, reqs[i].Arg)
				}

				respBody, err := msgpack.Marshal(resps[i])
				if err != nil {
					t.Fatal(err)
				}

				err = ws.WriteMsg(ctx, &pbv1.Message{Body: respBody})
				if err != nil {
					t.Fatal(err)
				}
				i++
			}
		})

		req := genrpc.NewBidirStreamReq[testReq, testResp](newTestStreamer(t, s2, "test/0.0.0/test"), "test", "test")
		req.SetHeader("test", []byte("test"))

		reqC := make(chan *testReq)
		go func() {
			for _, r := range reqs {
				reqC <- r
			}
			close(reqC)
		}()

		respC, err := req.Execute(context.Background(), s1.LocalPeer(), reqC)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		for resp := range respC {
			if resp.Arg != resps[i].Arg {
				t.Fatalf("unexpected response: got %s, want %s", resp.Arg, resps[i].Arg)
			}
			i++
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		tReq := &testReq{Arg: "test"}
		tErr := errors.New("test")

		s1, s2 := createClientServer(t, func(s network.Stream) {
			defer s.Close()

			ws := genrpc.WrapStream(s)
			ctx := context.Background()

			reqMsg, err := ws.ReadMsg(ctx)
			if err != nil {
				t.Fatal(err)
			}

			req := new(testReq)

			err = msgpack.Unmarshal(reqMsg.Body, req)
			if err != nil {
				t.Fatal(err)
			}

			if req.Arg != tReq.Arg {
				t.Fatalf("unexpected request: got %s, want %s", req.Arg, tReq.Arg)
			}

			err = ws.WriteMsg(ctx, &pbv1.Message{Error: tErr.Error()})
			if err != nil {
				t.Fatal(err)
			}
		})

		req := genrpc.NewBidirStreamReq[testReq, testResp](
			newTestStreamer(t, s2, "test/2.1.0/test"),
			"test",
			"test",
			"2.1.0",
		)

		reqC := make(chan *testReq)
		go func() {
			reqC <- tReq
			close(reqC)
		}()

		_, err := req.Execute(context.Background(), s1.LocalPeer(), reqC)
		if !strings.Contains(err.Error(), tErr.Error()) {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

package genrpc_test

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	genrpc "github.com/plexsysio/go-libp2p-genrpc"
)

func TestMux(t *testing.T) {
	t.Parallel()

	t.Run("new", func(t *testing.T) {
		t.Parallel()

		_ = genrpc.New("test")
	})

	t.Run("new with version", func(t *testing.T) {
		t.Parallel()

		t.Run("invalid", func(t *testing.T) {
			t.Parallel()

			_, err := genrpc.NewWithVersion("test", "testversion")
			if err == nil {
				t.Fatalf("expected invalid version error")
			}
		})

		t.Run("valid", func(t *testing.T) {
			t.Parallel()

			_, err := genrpc.NewWithVersion("test", "0.0.1")
			if err != nil {
				t.Fatalf("unexpected error: got %v", err)
			}
		})
	})

	t.Run("handle and register", func(t *testing.T) {
		t.Parallel()

		m, err := genrpc.NewWithVersion("test", "1.0.0")
		if err != nil {
			t.Fatal(err)
		}

		m.Handle("a", func(s network.Stream) {})

		host := &testHost{}
		genrpc.Register(host, m)

		hdlr, found := host.handler("test/1.0.0/a")
		if !found {
			t.Fatal("handler not found")
		}

		if !hdlr.matcher(protocol.ID("test/1.0.0/a")) {
			t.Fatal("unexpected matcher failure")
		}
		if !hdlr.matcher(protocol.ID("test/1.0.1/a")) {
			t.Fatal("unexpected matcher failure")
		}
		if hdlr.matcher(protocol.ID("test/2.0.0/a")) {
			t.Fatal("expected matcher failure")
		}
		if hdlr.matcher(protocol.ID("test")) {
			t.Fatal("expected matcher failure")
		}
		if hdlr.matcher(protocol.ID("test/100/a")) {
			t.Fatal("expected matcher failure")
		}
	})
}

type testHandler struct {
	matcher func(protocol.ID) bool
	handler func(network.Stream)
}

type testHost struct {
	handlers map[string]testHandler
}

func (t *testHost) SetStreamHandlerMatch(p protocol.ID, matcher func(check protocol.ID) bool, handler network.StreamHandler) {
	if t.handlers == nil {
		t.handlers = make(map[string]testHandler)
	}

	t.handlers[string(p)] = testHandler{matcher: matcher, handler: handler}
	return
}

func (t *testHost) handler(path string) (testHandler, bool) {
	testHandler, found := t.handlers[path]
	return testHandler, found
}

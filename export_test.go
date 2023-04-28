package genrpc

import (
	"context"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var WrapStream = wrapStream

type StreamerFunc func(context.Context, peer.ID, ...protocol.ID) (network.Stream, error)

func (f StreamerFunc) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	return f(ctx, p, pids...)
}

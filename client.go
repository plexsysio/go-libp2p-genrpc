package genrpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/coreos/go-semver/semver"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	pbv1 "github.com/plexsysio/go-libp2p-genrpc/gen/v1"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
)

// Streamer interface provides functionality to open a new stream. This is main
// functionality that is required by the RPCs to perform the RPCs.
type Streamer interface {
	NewStream(context.Context, peer.ID, ...protocol.ID) (network.Stream, error)
}

// Header interface provides functionality to add headers to the requests
type Header interface {
	SetHeader(key string, value []byte)
}

// baseRequest provides the common functionality for all the requests
type baseRequest struct {
	h       Streamer
	service string
	version *semver.Version
	path    string
	hdrs    map[string][]byte
}

func (b *baseRequest) SetHeader(key string, value []byte) {
	if b.hdrs == nil {
		b.hdrs = make(map[string][]byte)
	}
	b.hdrs[key] = value
}

func (b *baseRequest) prepare(ctx context.Context, p peer.ID) (network.Stream, error) {
	protoID := protocol.ID(fmt.Sprintf("%s/%s/%s", b.service, b.version.String(), b.path))

	str, err := b.h.NewStream(ctx, p, protoID)
	if err != nil {
		return nil, fmt.Errorf("failed opening NewStream: %w", err)
	}

	return str, nil
}

func getPathAndVersion(args []string) (string, *semver.Version) {
	if len(args) == 0 {
		panic("path is required")
	}
	version := semver.New("0.0.0")
	if len(args) > 1 {
		version = semver.New(args[1])
	}

	return args[0], version
}

// UnaryRequest is used to call a unary RPC registered on the server. The client
// is expected to send a single request and wait for a single response. The
// server can send a single response or an error. The client can send headers
// along with the request. The request and response types are expected to be
// same as the ones registered on the server.
type UnaryRequest[Req any, Resp any] interface {
	// Header interface provides functionality to add headers to the requests
	Header
	// Execute performs the unary RPC. The request is tied to a service and a
	// a path. The client can execute this request on any peer that supports
	// the service and the path. The request can be reused.
	Execute(context.Context, peer.ID, *Req) (*Resp, error)
}

// NewUnaryReq initializes a new UnaryRequest. The service and path are required
// to be passed as arguments. The version is optional and defaults to 0.0.0. Path is
// the path of the RPC. The path is used to identify the RPC on the server. The path
// is the first argument in the args. The version is the second argument in the args.
func NewUnaryReq[Req any, Resp any](
	h Streamer,
	service string,
	args ...string,
) UnaryRequest[Req, Resp] {
	path, version := getPathAndVersion(args)
	return &unaryReq[Req, Resp]{
		baseRequest: &baseRequest{
			h:       h,
			service: service,
			version: version,
			path:    path,
		},
	}
}

type unaryReq[Req any, Resp any] struct {
	*baseRequest
}

func (u *unaryReq[Req, Resp]) Execute(
	ctx context.Context,
	p peer.ID,
	req *Req,
) (*Resp, error) {
	reqBuf, err := msgpack.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling req: %w", err)
	}

	str, err := u.prepare(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("failed opening NewStream: %w", err)
	}
	defer str.Close()

	sWrap := wrapStream(str)
	err = sWrap.WriteMsg(ctx, &pbv1.Message{
		Headers: u.hdrs,
		Body:    reqBuf,
	})
	if err != nil {
		return nil, fmt.Errorf("failed writing req: %w", err)
	}

	respMsg, err := sWrap.ReadMsg(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed reading response: %w", err)
	}

	if len(respMsg.Error) > 0 {
		return nil, errors.New(respMsg.Error)
	}

	resp := new(Resp)
	err = msgpack.Unmarshal(respMsg.Body, resp)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshaling response: %w", err)
	}

	return resp, nil
}

// UpStreamRequest is used to call a up-stream RPC registered on the server. The
// client is expected to send a stream of requests and wait for a single response.
// The server can send a single response or an error. The client can send headers
// along with the request. The request and response types are expected to be
// same as the ones registered on the server.
type UpStreamRequest[Req any, Resp any] interface {
	// Header interface provides functionality to add headers to the requests
	Header
	// Execute performs the up-stream RPC. The request is tied to a service and a
	// a path. The client can execute this request on any peer that supports
	// the service and the path. The request can be reused. Execute blocks until
	// the server sends a response or an error, so the client is expected to asynchronously
	// pump the requests.
	Execute(context.Context, peer.ID, <-chan *Req) (*Resp, error)
}

// NewUpStreamReq initializes a new UpStreamRequest. The service and path are required
// to be passed as arguments. The version is optional and defaults to 0.0.0. Path is
// the path of the RPC. The path is used to identify the RPC on the server. The path
// is the first argument in the args. The version is the second argument in the args.
func NewUpStreamReq[Req any, Resp any](
	h Streamer,
	service string,
	args ...string,
) UpStreamRequest[Req, Resp] {
	path, version := getPathAndVersion(args)
	return &upStreamReq[Req, Resp]{
		baseRequest: &baseRequest{
			h:       h,
			service: service,
			version: version,
			path:    path,
		},
	}
}

type upStreamReq[Req any, Resp any] struct {
	*baseRequest
}

func (u *upStreamReq[Req, Resp]) Execute(
	ctx context.Context,
	p peer.ID,
	reqC <-chan *Req,
) (*Resp, error) {
	str, err := u.prepare(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("failed opening NewStream: %w", err)
	}
	defer str.Close()

	sWrap := wrapStream(str)
	for req := range reqC {
		reqBuf, err := msgpack.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("failed marshaling req: %w", err)
		}
		err = sWrap.WriteMsg(ctx, &pbv1.Message{
			Headers: u.hdrs,
			Body:    reqBuf,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to write req: %w", err)
		}
	}

	// this is done to signal to the client that the upstream is done
	_ = str.CloseWrite()

	respMsg, err := sWrap.ReadMsg(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed reading response: %w", err)
	}

	if len(respMsg.Error) > 0 {
		return nil, errors.New(respMsg.Error)
	}

	resp := new(Resp)
	err = msgpack.Unmarshal(respMsg.Body, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal msg: %w", err)
	}

	return resp, nil
}

// DownStreamRequest is used to call a down-stream RPC registered on the server. The
// client is expected to send a single request and wait for a stream of responses.
// If the error is non-nil, the client can expect no more responses. The client can
// send headers along with the request. The request and response types are expected
// to be same as the ones registered on the server.
type DownStreamRequest[Req any, Resp any] interface {
	// Header interface provides functionality to add headers to the requests
	Header
	// Execute performs the down-stream RPC. The request is tied to a service and a
	// a path. The client can execute this request on any peer that supports
	// the service and the path. The request can be reused. The response channel
	// is closed by the server when it is done sending responses.
	Execute(context.Context, peer.ID, *Req) (<-chan *Resp, error)
}

// NewDownStreamReq initializes a new DownStreamRequest. The service and path are required
// to be passed as arguments. The version is optional and defaults to 0.0.0. Path is
// the path of the RPC. The path is used to identify the RPC on the server. The path
// is the first argument in the args. The version is the second argument in the args.
func NewDownStreamReq[Req any, Resp any](
	h Streamer,
	service string,
	args ...string,
) DownStreamRequest[Req, Resp] {
	path, version := getPathAndVersion(args)
	return &downStreamReq[Req, Resp]{
		baseRequest: &baseRequest{
			h:       h,
			service: service,
			version: version,
			path:    path,
		},
	}
}

type downStreamReq[Req any, Resp any] struct {
	*baseRequest
}

func (d *downStreamReq[Req, Resp]) Execute(
	ctx context.Context,
	p peer.ID, req *Req,
) (<-chan *Resp, error) {
	reqBuf, err := msgpack.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling req: %w", err)
	}

	str, err := d.prepare(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("failed opening NewStream: %w", err)
	}

	sWrap := wrapStream(str)
	err = sWrap.WriteMsg(ctx, &pbv1.Message{
		Headers: d.hdrs,
		Body:    reqBuf,
	})
	if err != nil {
		return nil, fmt.Errorf("failed writing req: %w", err)
	}

	msg, err := sWrap.ReadMsg(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed reading response: %w", err)
	}
	if len(msg.Error) > 0 {
		return nil, errors.New(msg.Error)
	}

	respC := make(chan *Resp)
	go func() {
		defer close(respC)
		defer str.Close()

		for {
			resp := new(Resp)
			err = msgpack.Unmarshal(msg.Body, resp)
			if err != nil {
				return
			}

			select {
			case <-ctx.Done():
			case respC <- resp:
			}

			msg, err = sWrap.ReadMsg(ctx)
			if err != nil {
				return
			}
		}
	}()

	return respC, nil
}

// BidirStreamRequest is used to call a bidirectional RPC registered on the server.
// The client can send a stream of requests and receive a stream of responses. The
// client can send headers along with the request. The request and response types
// are expected to be same as the ones registered on the server.
type BidirStreamRequest[Req any, Resp any] interface {
	// Header interface provides functionality to add headers to the requests
	Header
	// Execute performs the bidirectional RPC. The request is tied to a service and a
	// a path. The client can execute this request on any peer that supports
	// the service and the path. The request can be reused. The client can close the
	// request channel to signal to the server that it is done sending requests. The
	// server can send error in the start. If the server sends error, the response
	// channel will be nil. The response channel is closed by the server
	// when it is done sending responses. The client can send headers along with the
	// request. Execute blocks untill the first response is received, so client is
	// expected to start pumping the requests before calling Execute.
	Execute(context.Context, peer.ID, <-chan *Req) (<-chan *Resp, error)
}

// NewBidirStreamReq initializes a new BidirStreamRequest
func NewBidirStreamReq[Req any, Resp any](
	h Streamer,
	service string,
	args ...string,
) BidirStreamRequest[Req, Resp] {
	path, version := getPathAndVersion(args)
	return &bidirStreamReq[Req, Resp]{
		baseRequest: &baseRequest{
			h:       h,
			service: service,
			version: version,
			path:    path,
		},
	}
}

type bidirStreamReq[Req any, Resp any] struct {
	*baseRequest
}

func (b *bidirStreamReq[Req, Resp]) Execute(
	ctx context.Context,
	p peer.ID,
	reqC <-chan *Req,
) (<-chan *Resp, error) {
	str, err := b.prepare(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("failed opening NewStream: %w", err)
	}

	eg, egCtx := errgroup.WithContext(ctx)

	sWrap := wrapStream(str)
	eg.Go(func() error {
		defer str.CloseWrite()

		for req := range reqC {
			reqBuf, err := msgpack.Marshal(req)
			if err != nil {
				return fmt.Errorf("failed marshaling req: %w", err)
			}
			err = sWrap.WriteMsg(ctx, &pbv1.Message{
				Headers: b.hdrs,
				Body:    reqBuf,
			})
			if err != nil {
				return fmt.Errorf("failed to write req: %w", err)
			}
		}
		return nil
	})

	msg, err := sWrap.ReadMsg(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed reading response: %w", err)
	}
	if len(msg.Error) > 0 {
		return nil, errors.New(msg.Error)
	}

	respC := make(chan *Resp)
	eg.Go(func() error {
		defer close(respC)

		for {
			resp := new(Resp)
			err = msgpack.Unmarshal(msg.Body, resp)
			if err != nil {
				return err
			}

			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case respC <- resp:
			}

			msg, err = sWrap.ReadMsg(egCtx)
			if err != nil {
				return nil
			}
		}
	})

	go func() {
		_ = eg.Wait()
		str.Close()
	}()

	return respC, nil
}

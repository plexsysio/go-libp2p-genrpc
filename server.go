package genrpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	pbv1 "github.com/plexsysio/go-libp2p-genrpc/gen/v1"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// wrappedStream is a wrapper around a network stream that provides methods to
// read and write messages. It also provides a context aware read and write
// methods that observe the context. If the context is canceled, the read or
// write operation is canceled.
type wrappedStream struct {
	r msgio.ReadCloser
	w msgio.WriteCloser
}

func wrapStream(s network.Stream) *wrappedStream {
	return &wrappedStream{
		r: msgio.NewVarintReaderSize(s, network.MessageSizeMax),
		w: msgio.NewVarintWriter(s),
	}
}

// ReadMsg reads a message from the stream and observes the context. If the context
// is canceled, the function returns with the error. If the context is not canceled,
// the function blocks until a message is received or the context is canceled.
func (w *wrappedStream) ReadMsg(ctx context.Context) (*pbv1.Message, error) {
	type result struct {
		msgBuf []byte
		err    error
	}

	resultC := make(chan result, 1)
	go func() {
		msgBuf, err := w.r.ReadMsg()
		resultC <- result{msgBuf: msgBuf, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-resultC:
		if res.err != nil {
			return nil, fmt.Errorf("failed to read msg: %w", res.err)
		}
		reqMsg := new(pbv1.Message)
		err := proto.Unmarshal(res.msgBuf, reqMsg)
		if err != nil {
			return nil, fmt.Errorf("failed unmarshling msg: %w", err)
		}
		return reqMsg, nil
	}
}

// WriteMsg writes a message to the stream and observes the context. If the context
// is canceled, the function returns with the error. If the context is not canceled,
// the function blocks until the message is written or the context is canceled.
func (w *wrappedStream) WriteMsg(ctx context.Context, msg *pbv1.Message) error {
	respBuf, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}

	errC := make(chan error, 1)
	go func() {
		errC <- w.w.WriteMsg(respBuf)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errC:
		return err
	}
}

type (
	// UnaryHandlerFunc is the signature of the handler function for unary RPCs.
	// It provides access to the headers and the raw request. This type allows
	// us to write middlewares for unary RPCs.
	UnaryHandlerFunc func(context.Context, map[string][]byte, []byte) error
	// UnaryMiddleware is the signature of the middleware function for unary RPCs.
	UnaryMiddleware func(UnaryHandlerFunc) UnaryHandlerFunc

	// StreamHandlerFunc is the signature of the handler function for streaming RPCs.
	// It provides access to the headers and the raw request stream. This type allows
	// us to write middlewares for streaming RPCs.
	StreamHandlerFunc func(context.Context, map[string][]byte, <-chan []byte) error
	// StreamMiddleware is the signature of the middleware function for streaming RPCs.
	StreamMiddleware func(StreamHandlerFunc) StreamHandlerFunc
)

type (
	peerKey struct{}
)

// GetPeerID returns the peer id of the remote peer from the context.
func GetPeerID(ctx context.Context) (peer.ID, error) {
	peer, ok := ctx.Value(peerKey{}).(peer.ID)
	if !ok {
		return "", errors.New("failed to get peer id from context")
	}
	return peer, nil
}

// SetPeerID sets the peer id of the remote peer in the context.
func SetPeerID(ctx context.Context, peerID peer.ID) context.Context {
	return context.WithValue(ctx, peerKey{}, peerID)
}

// Unary function provides a network.StreamHandler wrapped with message handling
// for unary request type APIs. Each API can be wrapped with middlewares to provide
// additional functionality. The middlewares are applied in the order they are passed.
// The Req and Resp types are the request and response types respectively. Currently
// the request and response types are expected to be go structs that can be marshaled
// and unmarshaled using msgpack. This means only exported fields will be sent.
// The error returned by the handler is sent back to the client as a new message.
func Unary[Req any, Resp any](
	handlerFn func(context.Context, *Req) (*Resp, error),
	mws ...UnaryMiddleware,
) network.StreamHandler {
	return func(stream network.Stream) {
		sWrap := wrapStream(stream)
		defer stream.Close()

		finalHandler := func(ctx context.Context, _ map[string][]byte, body []byte) error {
			req := new(Req)
			err := msgpack.Unmarshal(body, req)
			if err != nil {
				return fmt.Errorf("failed unmarshaling message: %w", err)
			}

			resp, err := handlerFn(ctx, req)
			if err != nil {
				return fmt.Errorf("failed in handler: %w", err)
			}

			respBody, err := msgpack.Marshal(resp)
			if err != nil {
				return fmt.Errorf("failed marshaling response: %w", err)
			}

			return sWrap.WriteMsg(ctx, &pbv1.Message{Body: respBody})
		}

		ctx := SetPeerID(context.Background(), stream.Conn().RemotePeer())
		reqMsg, err := sWrap.ReadMsg(ctx)
		if err != nil {
			return
		}

		var handler UnaryHandlerFunc = finalHandler
		for _, mw := range mws {
			handler = mw(handler)
		}

		err = handler(ctx, reqMsg.Headers, reqMsg.Body)
		if err != nil {
			_ = sWrap.WriteMsg(context.Background(), &pbv1.Message{Error: err.Error()})
		}
	}
}

// UpStream function provides a network.StreamHandler wrapped with message handling
// for streaming request type APIs. Each API can be wrapped with middlewares to provide
// additional functionality. The middlewares are applied in the order they are passed.
// The Req and Resp types are the request and response types respectively. Currently
// the request and response types are expected to be go structs that can be marshaled
// and unmarshaled using msgpack. This means only exported fields will be sent.
// The error returned by the handler is sent back to the client as a new message. The
// handler function is expected to read from the request channel till it is closed or
// the context is canceled. The handler function is expected to send the response back
// at the end of the operation. An error can be sent earlier if the operation fails.
// The client will receive the error and the stream will be closed. Client should send
// the headers only in the first message. If they are sent in subsequent messages, they
// will be ignored.
func UpStream[Req any, Resp any](
	handlerFn func(context.Context, <-chan *Req) (*Resp, error),
	mws ...StreamMiddleware,
) network.StreamHandler {
	return func(stream network.Stream) {
		sWrap := wrapStream(stream)
		defer stream.Close()

		ctx := SetPeerID(context.Background(), stream.Conn().RemotePeer())
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		eg, egCtx := errgroup.WithContext(ctx)

		finalHandler := func(ctx context.Context, _ map[string][]byte, reqBufs <-chan []byte) error {
			reqStr := make(chan *Req)
			eg.Go(func() error {
				defer close(reqStr)
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case buf, more := <-reqBufs:
						if !more {
							return nil
						}
						req := new(Req)
						err := msgpack.Unmarshal(buf, req)
						if err != nil {
							return err
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						case reqStr <- req:
						}
					}
				}
			})

			resp, err := handlerFn(ctx, reqStr)
			if err != nil {
				return err
			}

			if err := eg.Wait(); err != nil {
				return fmt.Errorf("failed during handling: %w", err)
			}

			respBody, err := msgpack.Marshal(resp)
			if err != nil {
				return fmt.Errorf("failed marshaling response: %w", err)
			}

			return sWrap.WriteMsg(context.Background(), &pbv1.Message{Body: respBody})
		}

		var handler StreamHandlerFunc = finalHandler
		for _, mw := range mws {
			handler = mw(handler)
		}

		reqBufs := make(chan []byte)
		hdrs := make(chan map[string][]byte, 1)

		eg.Go(func() error {
			defer close(reqBufs)

			for {
				msg, err := sWrap.ReadMsg(egCtx)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}

				select {
				// only first msg will contain headers
				case hdrs <- msg.Headers:
				default:
				}

				select {
				case reqBufs <- msg.Body:
				case <-egCtx.Done():
					return egCtx.Err()
				}
			}
		})

		err := handler(egCtx, <-hdrs, reqBufs)
		if err != nil {
			errors.Join(
				err,
				sWrap.WriteMsg(context.Background(), &pbv1.Message{Error: err.Error()}),
			)
		}
	}
}

// DownStream provides a network.StreamHandler wrapped with the message handling
// required for streaming response type APIs. Each API can be wrapped with middlewares
// to provide additional functionality. The middlewares are applied in the order they
// are passed. The Req and Resp types are the request and response types respectively.
// Currently the request and response types are expected to be go structs that can be
// marshaled and unmarshaled using msgpack. This means only exported fields will be sent.
// The handler function is expected to close the response channel when the operation
// is complete. The handler can only return error on start. Any error during the operation
// should be communicated via the response channel.
func DownStream[Req any, Resp any](
	handler func(context.Context, *Req) (<-chan *Resp, error),
	mws ...UnaryMiddleware,
) network.StreamHandler {
	return func(stream network.Stream) {
		sWrap := wrapStream(stream)
		defer stream.Close()

		finalHandler := func(ctx context.Context, _ map[string][]byte, body []byte) error {
			req := new(Req)
			err := msgpack.Unmarshal(body, req)
			if err != nil {
				return fmt.Errorf("failed unmarshaling message: %w", err)
			}

			respCh, err := handler(ctx, req)
			if err != nil {
				return fmt.Errorf("failed in handler: %w", err)
			}

			for resp := range respCh {
				respBody, err := msgpack.Marshal(resp)
				if err != nil {
					return fmt.Errorf("failed marshaling response: %w", err)
				}
				err = sWrap.WriteMsg(ctx, &pbv1.Message{Body: respBody})
				if err != nil {
					return fmt.Errorf("failed to write resp: %w", err)
				}
			}

			return nil
		}

		ctx := SetPeerID(context.Background(), stream.Conn().RemotePeer())
		reqMsg, err := sWrap.ReadMsg(ctx)
		if err != nil {
			return
		}

		var handler UnaryHandlerFunc = finalHandler
		for _, mw := range mws {
			handler = mw(handler)
		}

		err = handler(ctx, reqMsg.Headers, reqMsg.Body)
		if err != nil {
			_ = sWrap.WriteMsg(context.Background(), &pbv1.Message{Error: err.Error()})
		}
	}
}

// BidirStream provides a network.StreamHandler wrapped with message handling
// required for bidirectional streaming APIs. Each API can be wrapped with middlewares
// to provide additional functionality. The middlewares are applied in the order they
// are passed. The Req and Resp types are the request and response types respectively.
// Currently the request and response types are expected to be go structs that can be
// marshaled and unmarshaled using msgpack. This means only exported fields will be sent.
// The handler function is expected to read from the request channel untill it is closed.
// The handler can only return error on start. Any error during the operation should be
// communicated via the response channel. The handler is expected to close the response
// channel when the operation is complete. Bidirectional streaming messages can be
// tricky for the underlying transport. Ideally the client and the handler should
// send and receive messages in a synchronized manner. If the client sends a message
// and the handler does not read it, the client will block. If the handler sends a
// message and the client does not read it, the handler will block.
func BidirStream[Req any, Resp any](
	handlerFn func(context.Context, <-chan *Req) (<-chan *Resp, error),
	mws ...StreamMiddleware,
) network.StreamHandler {
	return func(stream network.Stream) {
		sWrap := wrapStream(stream)
		defer stream.Close()

		ctx := SetPeerID(context.Background(), stream.Conn().RemotePeer())
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		eg, egCtx := errgroup.WithContext(ctx)

		finalHandler := func(ctx context.Context, _ map[string][]byte, reqBufs <-chan []byte) error {
			reqStr := make(chan *Req)
			eg.Go(func() error {
				defer close(reqStr)
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case buf, more := <-reqBufs:
						if !more {
							return nil
						}
						req := new(Req)
						err := msgpack.Unmarshal(buf, req)
						if err != nil {
							return err
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						case reqStr <- req:
						}
					}
				}
			})

			respCh, err := handlerFn(ctx, reqStr)
			if err != nil {
				return err
			}

			eg.Go(func() error {
				for resp := range respCh {
					respBody, err := msgpack.Marshal(resp)
					if err != nil {
						return fmt.Errorf("failed marshaling response: %w", err)
					}

					err = sWrap.WriteMsg(ctx, &pbv1.Message{Body: respBody})
					if err != nil {
						return fmt.Errorf("failed writing response: %w", err)
					}
				}
				return nil
			})

			if err := eg.Wait(); err != nil {
				return fmt.Errorf("failed during handling: %w", err)
			}

			return nil
		}

		var handler StreamHandlerFunc = finalHandler
		for _, mw := range mws {
			handler = mw(handler)
		}

		reqBufs := make(chan []byte)
		hdrs := make(chan map[string][]byte, 1)

		eg.Go(func() error {
			defer close(reqBufs)

			for {
				msg, err := sWrap.ReadMsg(egCtx)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}

				select {
				// only first msg will contain headers
				case hdrs <- msg.Headers:
				default:
				}

				select {
				case reqBufs <- msg.Body:
				case <-egCtx.Done():
					return egCtx.Err()
				}
			}
		})

		err := handler(egCtx, <-hdrs, reqBufs)
		if err != nil {
			_ = sWrap.WriteMsg(context.Background(), &pbv1.Message{Error: err.Error()})
		}
	}
}

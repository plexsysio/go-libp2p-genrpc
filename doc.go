// Package genrpc is heavily inspired by the go-libp2p-gorpc package. It tries
// to provide a similar API to the go-libp2p-gorpc package, but with the use of
// generics. With generics we can provide a more type-safe API.
//
// Aim was to see if we can use generics to remove all the reflection code and still
// provide a similar API. The result is a bit more verbose, but it is type-safe. The
// API is also a bit different, but it is still easy to use and inspired by the
// net/http package.
//
// Differences with go-libp2p-gorpc:
//   - Each service and rpc has its own protocol ID. This allows us to support
//     multiple versions of the same service. The protocol ID is of the form:
//     /<service-name>/<version>/<rpc>. The version is a semantic version string.
//     The rpc is the string path which is used to register the handler. This could be
//     a method name or a path describing the method. As each rpc has its own protocol
//     ID, it also allows us to use the underlying libp2p observability (for eg. resource
//     manager dashboards etc).
//   - Middlewares can be registered per rpc. This allows us to have different
//     middlewares for different rpcs.
//   - There are four types of RPCs:
//     1. Request-Response: The client sends a request and waits for a response.
//     2. Request-Stream: The client sends a request and gets a stream of responses.
//     3. Stream-Request: The client sends a stream of requests and gets a response.
//     4. Stream-Stream: The client sends a stream of requests and gets a stream of responses.
//
// Users have to write the methods and register them with the server. Each method can
// be registered with a different path. The path is used to register the handler.
// The path can be a method name or a path describing the method. Each type of RPC
// has a specific signature. The method and object types dont need to be exported. Only
// the exported fields in the object are sent in the message. It uses msgpack for
// serialization, so go structs with exported fields can be used.
// The method can return an error. The error is sent back to the client
// as a response. Streams are mapped to channels. Closing the channel will close the
// stream.
// Corresponding to each RPC, there is a client method. The client method takes the
// same arguments as the server method. On the client side as well, streams
// are mapped to channels. Closing the channel will close the stream.
// Only thing required to start the server or client is a libp2p host. The server
// and client will use the host to start listening and dialing.
// Typical workflow:
//  1. Create a host.
//  2. Create a mux.
//  3. Register the methods with the mux.
//  4. Register the mux with the host.
//  5. Start the host.
//
// On the client side:
//  1. Create a host.
//  2. Create a request.
//  3. Execute the request with peer ID.
//
// The peer address information should be added to the Peerstore of the host prior
// to executing the request.
//
// Check examples for more details.
package genrpc

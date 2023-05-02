# go-libp2p-genrpc [![Go](https://github.com/plexsysio/go-libp2p-genrpc/workflows/Go/badge.svg)](https://github.com/plexsysio/go-libp2p-genrpc/actions) [![Go Reference](https://pkg.go.dev/badge/github.com/plexsysio/go-libp2p-genrpc.svg)](https://pkg.go.dev/github.com/plexsysio/go-libp2p-genrpc) [![Coverage Status](https://coveralls.io/repos/github/plexsysio/go-libp2p-genrpc/badge.svg?branch=main)](https://coveralls.io/github/plexsysio/go-libp2p-genrpc?branch=main)
Generics based RPC framework using libp2p

`go-libp2p-genrpc` is a small library which provides a type-safe API to write go functions as RPCs over libp2p transport.

The goal was to see if we can provide similar functionality as `go-libp2p-gorpc` but using generics in a type-safe manner. The result is a slightly different API which is more verbose. Additional functionality like writing middlewares is also possible.

## Install
This module can be installed with `go get`:
```
> go get github.com/plexsysio/go-libp2p-genrpc
```

## Usage
Check [docs](https://pkg.go.dev/github.com/plexsysio/go-libp2p-genrpc).

There are also some [examples](https://github.com/plexsysio/go-libp2p-genrpc/tree/main/examples).

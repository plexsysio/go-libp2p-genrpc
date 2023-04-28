package genrpc

import (
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const defaultVersion string = "0.0.0"

// Mux provides a multiplexer per service and version. It can be used to register
// different stream handler endpoints.
type Mux struct {
	serviceName string
	version     *semver.Version
	handlers    map[string]network.StreamHandler
}

// New returns a Mux which uses the default semantic version. The default version
// is 0.0.0.
func New(svcName string) *Mux {
	m, _ := NewWithVersion(svcName, defaultVersion)
	return m
}

// NewWithVersion returns a Mux which uses the provided semantic version. The
// version must be a valid semantic version string.
func NewWithVersion(svcName, version string) (*Mux, error) {
	sver, err := semver.NewVersion(version)
	if err != nil {
		return nil, err
	}

	return &Mux{
		serviceName: svcName,
		version:     sver,
		handlers:    make(map[string]network.StreamHandler),
	}, nil
}

// Handle is used to register stream handler for a particular path. If a path is
// already registered, it will overwrite the handler for that path.
func (m *Mux) Handle(path string, handler network.StreamHandler) {
	m.handlers[path] = handler
}

func (m *Mux) matcher(check protocol.ID) bool {
	splits := strings.Split(string(check), "/")
	if len(splits) < 2 {
		return false
	}

	chVers, err := semver.NewVersion(splits[1])
	if err != nil {
		return false
	}

	return m.version.Major == chVers.Major && m.version.Minor >= chVers.Minor
}

func (m *Mux) protocolKey(path string) protocol.ID {
	vers := m.version.String()
	return protocol.ID(strings.Join([]string{m.serviceName, vers, path}, "/"))
}

// Libp2pHost is the interface required from libp2p.Host. This is done for mocking
// in the tests.
type Libp2pHost interface {
	SetStreamHandlerMatch(protocol.ID, func(protocol.ID) bool, network.StreamHandler)
}

// Register is used to register the Mux on a libp2p host.
func Register(h Libp2pHost, mux *Mux) {
	for k, v := range mux.handlers {
		h.SetStreamHandlerMatch(mux.protocolKey(k), mux.matcher, v)
	}
}

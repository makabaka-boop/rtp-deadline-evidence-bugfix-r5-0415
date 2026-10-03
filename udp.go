package rtpaudio

import (
	"context"
	"net"
)

// UDPServer reads fixed-profile RTP packets from one UDP socket and dispatches
// them by remote address.
type UDPServer struct {
	receiver *Receiver
	conn     *net.UDPConn
}

// ListenUDP binds addr and returns a UDP server.
func ListenUDP(addr string, receiver *Receiver) (*UDPServer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &UDPServer{receiver: receiver, conn: conn}, nil
}

// LocalAddr returns the bound UDP address.
func (s *UDPServer) LocalAddr() net.Addr { return s.conn.LocalAddr() }

// Close closes the underlying UDP socket.
func (s *UDPServer) Close() error { return s.conn.Close() }

// Serve reads packets until the socket is closed or ctx is canceled.
// Malformed or unsupported packets do not terminate Serve; HandlePacket
// records and returns their disposition.
func (s *UDPServer) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		_ = s.conn.Close()
	}()

	buf := make([]byte, 1500)
	for {
		n, remote, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		_ = s.receiver.HandleUDPPacket(remote, data)
	}
}

// HandleUDPPacket is useful for tests that construct a net.UDPAddr without
// opening a socket.
func (r *Receiver) HandleUDPPacket(remote *net.UDPAddr, data []byte) PacketStatus {
	return r.HandlePacket(SourceKey(remote.String()), data, r.clock.Now())
}

package generation

import (
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type leasedConn struct {
	N.ExtendedConn
	lease adapter.GenerationLease
	once  sync.Once
}

func newLeasedConn(conn net.Conn, lease adapter.GenerationLease) net.Conn {
	return &leasedConn{
		ExtendedConn: bufio.NewExtendedConn(conn),
		lease:        lease,
	}
}

func (c *leasedConn) Close() error {
	err := c.ExtendedConn.Close()
	c.once.Do(c.lease.Release)
	return err
}

func (c *leasedConn) ReaderReplaceable() bool { return true }
func (c *leasedConn) WriterReplaceable() bool { return true }
func (c *leasedConn) Upstream() any           { return c.ExtendedConn }

type leasedPacketConn struct {
	net.PacketConn
	lease adapter.GenerationLease
	once  sync.Once
}

func newLeasedPacketConn(conn net.PacketConn, lease adapter.GenerationLease) net.PacketConn {
	return &leasedPacketConn{
		PacketConn: conn,
		lease:      lease,
	}
}

func (c *leasedPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	if packetReader, loaded := c.PacketConn.(N.PacketReader); loaded {
		return packetReader.ReadPacket(buffer)
	}
	_, source, err := buffer.ReadPacketFrom(c.PacketConn)
	if err != nil {
		return M.Socksaddr{}, err
	}
	return M.SocksaddrFromNet(source).Unwrap(), nil
}

func (c *leasedPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	if packetWriter, loaded := c.PacketConn.(N.PacketWriter); loaded {
		return packetWriter.WritePacket(buffer, destination)
	}
	defer buffer.Release()
	_, err := c.PacketConn.WriteTo(buffer.Bytes(), destination.UDPAddr())
	return err
}

func (c *leasedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.lease.Release)
	return err
}

func (c *leasedPacketConn) ReaderReplaceable() bool { return true }
func (c *leasedPacketConn) WriterReplaceable() bool { return true }
func (c *leasedPacketConn) Upstream() any           { return bufio.NewPacketConn(c.PacketConn) }

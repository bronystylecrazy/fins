package fins

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// getAvailablePort returns an available port on localhost
func getAvailablePort(t *testing.T) int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to get available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}

// getTestAddresses returns a pair of available addresses for testing
func getTestAddresses(t *testing.T) (clientAddr, plcAddr Address) {
	clientPort := getAvailablePort(t)
	plcPort := getAvailablePort(t)

	// Ensure ports are different
	if clientPort == plcPort {
		plcPort = getAvailablePort(t)
	}

	clientAddr = NewAddress("127.0.0.1", clientPort, 0, 2, 0)
	plcAddr = NewAddress("127.0.0.1", plcPort, 0, 10, 0)

	t.Logf("Using client port %d, PLC port %d", clientPort, plcPort)
	return
}

func TestFinsClient(t *testing.T) {
	ctx := context.Background()
	clientAddr, plcAddr := getTestAddresses(t)

	toWrite := []uint16{5, 4, 3, 2, 1}

	s, e := NewPLCSimulator(plcAddr)
	if e != nil {
		panic(e)
	}
	defer s.Close()

	c, e := NewClient(clientAddr, plcAddr)
	if e != nil {
		panic(e)
	}
	defer c.Close()

	// ------------- Test Words
	err := c.WriteWords(ctx, MemoryAreaDMWord, 100, toWrite)
	assert.Nil(t, err)

	vals, err := c.ReadWords(ctx, MemoryAreaDMWord, 100, 5)
	assert.Nil(t, err)
	assert.Equal(t, toWrite, vals)

	// test setting response timeout
	c.SetTimeoutMs(50)
	_, err = c.ReadWords(ctx, MemoryAreaDMWord, 100, 5)
	assert.Nil(t, err)

	// ------------- Test Strings
	err = c.WriteString(ctx, MemoryAreaDMWord, 10, "ф1234")
	assert.Nil(t, err)

	v, err := c.ReadString(ctx, MemoryAreaDMWord, 12, 1)
	assert.Nil(t, err)
	assert.Equal(t, "12", v)

	v, err = c.ReadString(ctx, MemoryAreaDMWord, 10, 3)
	assert.Nil(t, err)
	assert.Equal(t, "ф1234", v)

	v, err = c.ReadString(ctx, MemoryAreaDMWord, 10, 5)
	assert.Nil(t, err)
	assert.Equal(t, "ф1234", v)

	// ------------- Test Bytes
	err = c.WriteBytes(ctx, MemoryAreaDMWord, 10, []byte{0x00, 0x00, 0xC1, 0xA0})
	assert.Nil(t, err)

	b, err := c.ReadBytes(ctx, MemoryAreaDMWord, 10, 2)
	assert.Nil(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0xC1, 0xA0}, b)

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(-20))
	err = c.WriteBytes(ctx, MemoryAreaDMWord, 10, buf)
	assert.Nil(t, err)

	b, err = c.ReadBytes(ctx, MemoryAreaDMWord, 10, 4)
	assert.Nil(t, err)
	assert.Equal(t, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x34, 0xc0}, b)

	// ------------- Test Bits
	err = c.WriteBits(ctx, MemoryAreaDMBit, 10, 2, []bool{true, false, true})
	assert.Nil(t, err)

	bs, err := c.ReadBits(ctx, MemoryAreaDMBit, 10, 2, 3)
	assert.Nil(t, err)
	assert.Equal(t, []bool{true, false, true}, bs)

	bs, err = c.ReadBits(ctx, MemoryAreaDMBit, 10, 1, 5)
	assert.Nil(t, err)
	assert.Equal(t, []bool{false, true, false, true, false}, bs)
}

func TestClientClosed(t *testing.T) {
	ctx := context.Background()
	clientAddr, plcAddr := getTestAddresses(t)

	s, e := NewPLCSimulator(plcAddr)
	if e != nil {
		panic(e)
	}
	defer s.Close()

	c, e := NewClient(clientAddr, plcAddr)
	if e != nil {
		panic(e)
	}

	assert.False(t, c.IsClosed())

	c.Close()

	assert.True(t, c.IsClosed())

	// Operations should return ClientClosedError
	_, err := c.ReadWords(ctx, MemoryAreaDMWord, 100, 5)
	assert.Error(t, err)
	assert.IsType(t, ClientClosedError{}, err)
}

func TestContextCancellation(t *testing.T) {
	clientAddr, plcAddr := getTestAddresses(t)

	s, e := NewPLCSimulator(plcAddr)
	if e != nil {
		panic(e)
	}
	defer s.Close()

	c, e := NewClient(clientAddr, plcAddr)
	if e != nil {
		panic(e)
	}
	defer c.Close()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Wait for context to expire
	time.Sleep(10 * time.Millisecond)

	// Operation should fail with context error
	_, err := c.ReadWords(ctx, MemoryAreaDMWord, 100, 5)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestContextCancellationImmediate(t *testing.T) {
	clientAddr, plcAddr := getTestAddresses(t)

	s, e := NewPLCSimulator(plcAddr)
	if e != nil {
		panic(e)
	}
	defer s.Close()

	c, e := NewClient(clientAddr, plcAddr)
	if e != nil {
		panic(e)
	}
	defer c.Close()

	// Create a context and cancel it immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Operation should fail with context.Canceled
	_, err := c.ReadWords(ctx, MemoryAreaDMWord, 100, 5)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestContextWithTimeout(t *testing.T) {
	ctx := context.Background()
	clientAddr, plcAddr := getTestAddresses(t)

	s, e := NewPLCSimulator(plcAddr)
	if e != nil {
		panic(e)
	}
	defer s.Close()

	c, e := NewClient(clientAddr, plcAddr)
	if e != nil {
		panic(e)
	}
	defer c.Close()

	// Create a context with a reasonable timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// Operation should succeed
	toWrite := []uint16{1, 2, 3}
	err := c.WriteWords(ctxWithTimeout, MemoryAreaDMWord, 200, toWrite)
	assert.Nil(t, err)

	vals, err := c.ReadWords(ctxWithTimeout, MemoryAreaDMWord, 200, 3)
	assert.Nil(t, err)
	assert.Equal(t, toWrite, vals)
}

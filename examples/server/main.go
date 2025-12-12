package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bronystylecrazy/fins"
)

func main() {
	host := flag.String("host", "127.0.0.1", "UDP host to bind the PLC simulator")
	port := flag.Int("port", 9600, "UDP port for the PLC simulator")
	network := flag.Int("network", 0, "FINS network number for the PLC")
	node := flag.Int("node", 10, "FINS node address for the PLC")
	unit := flag.Int("unit", 0, "FINS unit address for the PLC")
	flag.Parse()

	plcAddr := fins.NewAddress(*host, *port, byte(*network), byte(*node), byte(*unit))

	server, err := fins.NewPLCSimulator(plcAddr)
	if err != nil {
		log.Fatalf("failed to start PLC simulator: %v", err)
	}
	defer server.Close()

	go func() {
		if err := <-server.Err(); err != nil {
			log.Printf("server error: %v", err)
		}
	}()

	log.Printf("PLC simulator listening on %s (network=%d node=%d unit=%d)", plcAddr.UdpAddress.String(), plcAddr.FinAddress.Network, plcAddr.FinAddress.Node, plcAddr.FinAddress.Unit)

	// Wait for interrupt/terminate signal to exit cleanly.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down simulator...")
}

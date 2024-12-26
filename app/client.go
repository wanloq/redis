package main

import (
	"fmt"
	"io"
	"net"
	"strings"
)

func client() {
	// Connect to the TCP server on port 8080
	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to server")

	// Send some data to the server
	data := "Hello, server!"
	if _, err := io.Copy(conn, strings.NewReader(data)); err != nil {
		fmt.Println("Error sending data:", err)
		return
	}

	// Read the echoed data back from the server
	buf := make([]byte, len(data))
	if _, err := io.ReadFull(conn, buf); err != nil {
		fmt.Println("Error reading data:", err)
		return
	}

	fmt.Println("Received from server:", string(buf))
}

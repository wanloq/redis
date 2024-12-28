package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var p = fmt.Println

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	p("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		p("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	p("Listening on :6379...")

	for {
		conn, err := l.Accept()
		p("new connection created")
		if err != nil {
			p("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	// Ensure we close the connection after we're done
	defer conn.Close()

	// for {
	// Read data
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	msg := string(buf[:n])
	// p("Received: %q\n", msg)
	if err != nil {
		return
	}

	// Write new data
	commands := (strings.Split(msg, "\r\n"))
	// comm := len(strings.Split(msg, "\r\n"))
	if strings.ToUpper(commands[2]) == "PING" {
		p(msg, "PING COMMAND")
		conn.Write([]byte("+PONG\r\n"))
	} else if strings.ToUpper(commands[2]) == "ECHO" {
		p(msg, "ECHO COMMAND")
		// mLen, _ := strconv.Atoi((strings.Trim(commands[0], "*")))
		// mLen = mLen - 1
		// mLenStr := "*" + strconv.Itoa(mLen) + "\r\n"
		newMsg := fmt.Sprintf("*%d\r\n%s\r\n%s\r\n%s\r\n%s\r\n", len(commands[3]), commands[3], commands[4], commands[5], commands[6])
		// newMsg := mLenStr + strings.Join(commands[3:], "\r\n")
		p(newMsg)

		conn.Write([]byte(newMsg))
	} else {
		p("UNRECOGNIZED COMMAND")
		conn.Write([]byte("-ERR unrecognized command\r\n"))
	}
	// }
}

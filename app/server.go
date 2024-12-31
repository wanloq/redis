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
	// 	// Read data
	// 	buf := make([]byte, 1024)
	// 	_, err := conn.Read(buf)
	// 	if err != nil {
	// 		return
	// 	}

	// 	// Write new data
	// 	message := []byte("+PONG\r\n")
	// 	conn.Write(message)
	// }

	for {
		// Read data
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		msg := string(buf[:n])
		if err != nil {
			continue
		}

		// Write new data
		commands := (strings.Split(msg, "\r\n"))
		if strings.ToUpper(commands[2]) == "PING" {
			p(msg, "PING COMMAND")
			conn.Write([]byte("+PONG\r\n"))
		} else if strings.ToUpper(commands[2]) == "ECHO" {
			p(msg, "ECHO COMMAND")
			var mStr []string
			for i := 4; i < len(commands)-1; i += 2 {
				mStr = append(mStr, commands[i])
			}
			mLen := strings.Join(mStr, " ")
			// mLen, _ := strconv.Atoi((strings.Trim(commands[0], "*")))
			mComp := fmt.Sprintf("$%d\r\n", len(mLen))
			newMsg := fmt.Sprintf("%s\r\n%s\r\n", mComp, mStr[0:])
			// newMsg := mComp + strings.Join(commands[3:], "\r\n")
			p(mLen)
			p(newMsg)
			conn.Write([]byte(newMsg))
			// } else if strings.ToUpper(commands[2]) == "ECHO" {
			// 	p(msg, "ECHO COMMAND")
			// 	mLen, _ := strconv.Atoi((strings.Trim(commands[0], "*")))
			// 	mLen = mLen - 1
			// 	newMsg := ""
			// 	if mLen > 1 {
			// 		mLenStr := fmt.Sprintf("*%d\r\n", mLen)
			// 		newMsg = mLenStr + strings.Join(commands[3:], "\r\n")
			// 	} else {
			// 		newMsg = strings.Join(commands[3:], "\r\n")
			// 	}
			// 	conn.Write([]byte(newMsg))
		} else {
			p("UNRECOGNIZED COMMAND")
			conn.Write([]byte("-ERR unrecognized command\r\n"))
		}
	}
}

func bulkStrParse() {

}

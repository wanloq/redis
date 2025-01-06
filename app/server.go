package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var p = fmt.Println
var data = make(map[string]string)

var key, val, valLen, newMsg string
var cr = "\r\n"
var redisDir = "/tmp/redis-files"
var fileName = "dump.rdb"

// var msg = ""
// var msgStr = strings.Split(msg, cr)
// var comLen, _ = strconv.Atoi(strings.Trim(msgStr[0], "*"))
// var fChar = fmt.Sprintf("*%d", comLen-1)
// var com = strings.ToUpper(strings.Join(msgStr[2:3], "\r\n"))
// var arg = strings.ToUpper(strings.Join(msgStr[3:], "\r\n"))

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
	persistence(redisDir, fileName)

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

	for {
		// Read data
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		msg := string(buf[:n])
		if err != nil {
			continue
		}

		// Write new data
		newMsg := MyParser(msg)
		conn.Write([]byte(newMsg))

	}
}

func MyParser(msg string) string {
	// var key, val, valLen, newMsg string
	// cr := "\r\n"
	msgStr := strings.Split(msg, cr)
	comLen, _ := strconv.Atoi(strings.Trim(msgStr[0], "*"))
	fChar := fmt.Sprintf("*%d", comLen-1)
	com := strings.ToUpper(strings.Join(msgStr[2:3], "\r\n"))
	arg := strings.ToUpper(strings.Join(msgStr[3:], "\r\n"))

	switch {
	case com == "PING":
		newMsg = PingCommand()
	case com == "ECHO":
		newMsg = EchoCommand(fChar, arg)
	case com == "SET":
		newMsg = SetCommand(fChar, com, comLen, msgStr)
	case com == "GET":
		newMsg = GetCommand(fChar, com, comLen, msgStr)
	case com == "CONFIG":
		newMsg = ConfigGet(fChar, comLen, msgStr)
	default:
		newMsg = fmt.Sprintf("-ERR unrecognized command%s", cr)
	}
	// p("key:", key)
	// p("val:", val)
	// p("data:", data)
	// p("Arg:", arg)
	// p("MsgStr:", msgStr)
	// p("ComLen:", comLen)
	// p("NewMsg:", newMsg)
	return newMsg
}

func PingCommand() string {
	newMsg = fmt.Sprintf("+PONG%s", cr)
	return newMsg
}
func EchoCommand(fChar, arg string) string {
	newMsg = fmt.Sprintf("%s%s%s%s", fChar, cr, arg, cr)
	return newMsg
}
func SetCommand(fChar, com string, comLen int, msgStr []string) string {
	if com == "SET" {
		if comLen >= 3 {
			key = strings.ToUpper(msgStr[4])
			val = strings.ToUpper(msgStr[6])
			if comLen == 5 {
				exArg1 := strings.ToUpper(msgStr[8])
				exArg2 := msgStr[10]
				if exArg1 == "PX" {
					removeItem(key, data, exArg2)
					newMsg = fmt.Sprintf("+OK%s", cr)
				}
			} else {
				data[key] = val
				newMsg = fmt.Sprintf("+OK%s", cr)
			}
		}
	} else {
		newMsg = fmt.Sprintf("-ERR invalid number of arguments for the SET command%s", cr)
	}
	return newMsg
}
func GetCommand(fChar, com string, comLen int, msgStr []string) string {
	if com == "GET" {
		if comLen == 2 {
			key = strings.ToUpper(msgStr[4])
			val = data[key]
			if val != "" {
				valLen = fmt.Sprintf("$%d", len(val))
				newMsg = fmt.Sprintf("%s%s%s%s%s%s", fChar, cr, valLen, cr, val, cr)
			} else {
				newMsg = fmt.Sprintf("$-1%s", cr)
			}
		} else {
			newMsg = fmt.Sprintf("-ERR invalid number of arguments for this command%s", cr)
		}
	}
	return newMsg
}
func ConfigGet(fChar string, comLen int, msgStr []string) string {
	if strings.ToUpper(msgStr[4]) == "GET" && strings.ToUpper(msgStr[6]) == "DIR" {
		//*2\r\n $3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
		command := msgStr[6]
		lenCommand := fmt.Sprintf("$%d", len(command))
		dirName := "/tmp/redis-files" //fmt.Sprintf("")
		lenDirName := fmt.Sprintf("$%d", len(dirName))
		newMsg = fmt.Sprintf("%s%s%s%s%s%s%s%s%s%s", fChar, cr, lenCommand, cr, command, cr, lenDirName, cr, dirName, cr)
	} else if strings.ToUpper(msgStr[4]) == "GET" && strings.ToUpper(msgStr[6]) == "DBFILENAME" {
		command := msgStr[6]
		lenCommand := fmt.Sprintf("$%d", len(command))
		// fileName = "dump.rdb" //fmt.Sprintf("")
		lenDirName := fmt.Sprintf("$%d", len(fileName))
		newMsg = fmt.Sprintf("%s%s%s%s%s%s%s%s%s%s", fChar, cr, lenCommand, cr, command, cr, lenDirName, cr, fileName, cr)
	} else {
		newMsg = fmt.Sprintf("-ERR invalid number of arguments for this command%s", cr)
	}

	p("MsgStr:", msgStr)
	p("ComLen:", comLen)
	p("NewMsg:", newMsg)

	return newMsg
}

func removeItem(key string, mapName map[string]string, exArg2 string) {
	parsedTime, err := time.ParseDuration(exArg2)
	if err != nil {
		return
	}
	time.Sleep(parsedTime * time.Millisecond)
	delete(mapName, key)
	// return "Deleted"
}

func persistence(redisDir, fileName string) {
	//
	cmd := exec.Command("mkdir", redisDir)
	cmd.Dir = redisDir
	err := cmd.Run()
	if err != nil {
		log.Panic("error: ", err)
	}

	// err = os.Chdir(redisDir)
	// if err != nil {
	// 	log.Panic(err)
	// }
	_, err = os.Create("/" + fileName)
	if err != nil {
		log.Panic(err)
	}

}

func createFile(fileName string) {
	_, err := os.Create(fileName + ".rdb")
	if err != nil {
		log.Panic(err)
	}

}

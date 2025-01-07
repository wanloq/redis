package main

import (
	"fmt"
	"net"
	"os"
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
var redisDir = "./tmp/redis-files/"
var fileName = "dump.rdb"
var magicStr = "REDIS"
var verNum = "0011"
var redisVer = "7.0.15"

// var msg = ""
// var msgStr = strings.Split(msg, cr)
// var comLen, _ = strconv.Atoi(strings.Trim(msgStr[0], "*"))
// var fChar = fmt.Sprintf("*%d", comLen-1)
// var com = strings.ToUpper(strings.Join(msgStr[2:3], "\r\n"))
// var arg = strings.ToUpper(strings.Join(msgStr[3:], "\r\n"))

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	p("Logs from your program will appear here!")
	persistence(redisDir, fileName)

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
	case com == "SAVE":
		newMsg = SaveCommand(fChar, com, comLen, msgStr)
	default:
		newMsg = fmt.Sprintf("-ERR unrecognized command%s", cr)
	}
	// p("key:", key)
	// p("val:", val)
	// p("data:", data)
	// p("Arg:", arg)
	p("MsgStr:", msgStr)
	p("ComLen:", comLen)
	p("NewMsg:", newMsg)
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
		// dirName := redisDir //"/tmp/redis-files" //fmt.Sprintf("")
		lenDirName := fmt.Sprintf("$%d", len(redisDir))
		newMsg = fmt.Sprintf("%s%s%s%s%s%s%s%s%s%s", fChar, cr, lenCommand, cr, command, cr, lenDirName, cr, redisDir, cr)
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
func SaveCommand(fChar, com string, comLen int, msgStr []string) string {
	if com == "SAVE" {
		if fileExists(redisDir, fileName) {
			p("Updating file")
			createFile(redisDir, fileName)
			// oldData := ""
			// newData := ""
			// updateFile(redisDir+fileName, oldData, newData)
		} else {
			createFile(redisDir, fileName)
		}
		newMsg = fmt.Sprintf("+OK%s", cr)
	}
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
	if fileExists(redisDir, fileName) {
		p("DB loaded from disk:")
		rdbContent, _ := loadFileContent(redisDir, fileName)
		contentStr := string(rdbContent)
		p("Content:", contentStr)
	} else {
		p("No existing DB file on disk")
	}

	// p(magicStr)
	// return newMsg
}

func updateFile(filename string, oldData string, newData string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	newContent := strings.ReplaceAll(string(data), oldData, newData)

	err = os.WriteFile(filename, []byte(newContent), 0644)
	if err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}
	return nil
}

func createFile(redisDir, fileName string) {
	file, err := os.Create(redisDir + fileName)
	// file, err := os.OpenFile(redisDir+fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	//write header
	header := magicStr + verNum
	_, err = file.WriteString(header)
	if err != nil {
		fmt.Println("Error writing header:", err)
		return
	}
	//write metadata
	metadata := "redis-ver" + redisVer
	file.Write([]byte{0xFA})
	_, err = file.WriteString(metadata)
	if err != nil {
		fmt.Println("Error writing metadata:", err)
		return
	}
	//write DB
	file.Write([]byte{0xFE})
	file.Write([]byte{00})
	for key, value := range data {
		if err := writeKeyValue(file, key, value); err != nil {
			fmt.Println("Error writing key-value pair:", err)
			return
		}
	}
	_, err = file.WriteString(data[key])
	if err != nil {
		fmt.Println("Error writing content:", err)
		return
	}

	fmt.Println("RDB written successfully!")

}

func writeKeyValue(file *os.File, key, value string) error {
	// Write type: 0x00 for String type
	file.Write([]byte{0x00})

	// Write key length and key
	if err := writeLength(file, len(key)); err != nil {
		return err
	}
	_, err := file.WriteString(key)
	if err != nil {
		return err
	}

	// Write value length and value
	if err := writeLength(file, len(value)); err != nil {
		return err
	}
	_, err = file.WriteString(value)
	return err
}

func writeLength(file *os.File, length int) error {
	if length < (1 << 6) {
		// Single-byte encoding for small lengths
		_, err := file.Write([]byte{byte(length)})
		return err
	} else if length < (1 << 14) {
		// Two-byte encoding for medium lengths
		val := (length & 0x3FFF) | 0x4000
		_, err := file.Write([]byte{byte(val >> 8), byte(val)})
		return err
	} else {
		// Four-byte encoding for larger lengths
		_, err := file.Write([]byte{0x80, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		return err
	}
}

func loadFileContent(redisDir, filename string) ([]byte, error) {
	content, err := os.ReadFile(redisDir + filename)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %v", redisDir+filename, err)
	}
	return content, nil
}

func fileExists(redisDir, fileName string) bool {
	_, err := os.Stat(redisDir + fileName)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	fmt.Println("Error checking file:", err)
	return false
}

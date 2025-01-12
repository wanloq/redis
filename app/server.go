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
var respStr resp
var database DB

const (
	typeString = 0x00
	typeDB     = 0xFE
	eofMarker  = 0xFF
	dbIndex    = 00
)

type resp struct {
	Raw     string
	RawStr  []string
	Command string
	Data    string
	Arg     string
	Arg2    string
	ComLen  int
	DataLen int
	ArgLen  int
	Arg2Len int
	FChar   string
}

type DB struct {
	Key   string
	Value string
	PX    time.Duration
}

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

func debug() {

	p("key:", key)
	p("val:", val)
	p("MAP:", data)
	// p("RAW:", respStr.Raw)
	p("RAW STR SLICE:", respStr.RawStr)
	p("COMMAND:", respStr.Command)
	p("DATA:", respStr.Data)
	p("ARGUMENT:", respStr.Arg)
	p("COMLEN:", respStr.ComLen)
	p("DATALEN:", respStr.DataLen)
	p("ARGUMENTLEN:", respStr.ArgLen)
	p("FCHAR:", respStr.FChar)
	p("NEW MSG:", newMsg)
	// p("OTHER:", respStr)
}

func handleClient(conn net.Conn) {
	// Ensure we close the connection after we're done
	defer conn.Close()

	for {
		// Read data
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			continue
		}

		// Write new data
		newMsg := ProcessCommand(buf, n)
		conn.Write([]byte(newMsg))

	}
}

func MyParser(buf []byte, n int) resp {

	respStr.Raw = string(buf[:n])                                                //msg
	respStr.RawStr = strings.Split(respStr.Raw, cr)                              //msgstr
	respStr.Command = strings.ToUpper(strings.Join(respStr.RawStr[2:3], "\r\n")) //com
	respStr.Data = strings.ToUpper(strings.Join(respStr.RawStr[3:], "\r\n"))     //arg
	respStr.ComLen, _ = strconv.Atoi(strings.Trim(respStr.RawStr[0], "*"))       //comlen
	respStr.FChar = fmt.Sprintf("*%d", respStr.ComLen-1)                         //fchar

	return respStr
}

func ProcessCommand(buf []byte, n int) string {
	MyParser(buf, n)
	switch {
	case respStr.Command == "PING":
		newMsg = PingCommand()
	case respStr.Command == "ECHO":
		newMsg = EchoCommand(respStr.FChar, respStr.Data)
	case respStr.Command == "SET":
		newMsg = SetCommand(respStr.FChar, respStr.Command, respStr.ComLen, respStr.RawStr)
	case respStr.Command == "GET":
		newMsg = GetCommand(respStr.FChar, respStr.Command, respStr.ComLen, respStr.RawStr)
	case respStr.Command == "KEYS":
		newMsg = GetCommand(respStr.FChar, respStr.Command, respStr.ComLen, respStr.RawStr)
	case respStr.Command == "CONFIG":
		newMsg = ConfigGet(respStr.FChar, respStr.ComLen, respStr.RawStr)
	case respStr.Command == "SAVE":
		newMsg = SaveCommand(respStr.FChar, respStr.Command, respStr.ComLen, respStr.RawStr)
	default:
		newMsg = fmt.Sprintf("-ERR unrecognized command%s", cr)
	}
	return newMsg
}

func PingCommand() string {
	newMsg = fmt.Sprintf("+PONG%s", cr)
	return newMsg
}
func EchoCommand(fChar, arg string) string {
	newMsg = fmt.Sprintf("%s%s%s%s", respStr.FChar, cr, respStr.Data, cr)
	return newMsg
}
func SetCommand(fChar, com string, comLen int, msgStr []string) string {
	if respStr.ComLen >= 3 {
		key = respStr.RawStr[4]
		val = respStr.RawStr[6]
		if respStr.ComLen == 5 {
			respStr.Arg = strings.ToUpper(respStr.RawStr[8])
			respStr.Arg2 = respStr.RawStr[10] + "ms"
			if respStr.Arg == "PX" {
				data[key] = val
				p("removing item in", respStr.Arg2)
				go removeItem(key, data, respStr.Arg2)
				newMsg = fmt.Sprintf("+OK%s", cr)
			}
		} else {
			data[key] = val
			newMsg = fmt.Sprintf("+OK%s", cr)
		}
	}
	return newMsg
}
func GetCommand(fChar, com string, comLen int, msgStr []string) string {
	if respStr.ComLen == 2 {
		key = respStr.RawStr[4]
		val = data[key]
		if val != "" {
			valLen = fmt.Sprintf("$%d", len(val))
			newMsg = fmt.Sprintf("%s%s%s%s%s%s", respStr.FChar, cr, valLen, cr, val, cr)
		} else {
			newMsg = fmt.Sprintf("$-1%s", cr)
		}
	} else {
		newMsg = fmt.Sprintf("-ERR invalid number of arguments for this command%s", cr)
	}
	return newMsg
}
func KeysCommand(fChar, com string, comLen int, msgStr []string) string {
	if respStr.ComLen == 2 {
		key = respStr.RawStr[4]
		val = data[key]
		if val != "" {
			valLen = fmt.Sprintf("$%d", len(val))
			newMsg = fmt.Sprintf("%s%s%s%s%s%s", respStr.FChar, cr, valLen, cr, val, cr)
		} else {
			newMsg = fmt.Sprintf("$-1%s", cr)
		}
	} else {
		newMsg = fmt.Sprintf("-ERR invalid number of arguments for this command%s", cr)
	}
	return newMsg
}
func ConfigGet(fChar string, comLen int, msgStr []string) string {
	if strings.ToUpper(respStr.RawStr[4]) == "GET" && strings.ToUpper(respStr.RawStr[6]) == "DIR" {
		//*2\r\n $3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
		command := respStr.RawStr[6]
		lenCommand := fmt.Sprintf("$%d", len(command))
		// dirName := redisDir //"/tmp/redis-files" //fmt.Sprintf("")
		lenDirName := fmt.Sprintf("$%d", len(redisDir))
		newMsg = fmt.Sprintf("%s%s%s%s%s%s%s%s%s%s", respStr.FChar, cr, lenCommand, cr, command, cr, lenDirName, cr, redisDir, cr)
	} else if strings.ToUpper(respStr.RawStr[4]) == "GET" && strings.ToUpper(respStr.RawStr[6]) == "DBFILENAME" {
		command := respStr.RawStr[6]
		lenCommand := fmt.Sprintf("$%d", len(command))
		// fileName = "dump.rdb" //fmt.Sprintf("")
		lenDirName := fmt.Sprintf("$%d", len(fileName))
		newMsg = fmt.Sprintf("%s%s%s%s%s%s%s%s%s%s", respStr.FChar, cr, lenCommand, cr, command, cr, lenDirName, cr, fileName, cr)
	} else {
		newMsg = fmt.Sprintf("-ERR invalid number of arguments for this command%s", cr)
	}
	return newMsg
}
func SaveCommand(fChar, com string, comLen int, msgStr []string) string {
	// if com == "SAVE" {
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
	// }
	return newMsg
}

func removeItem(key string, mapName map[string]string, pxTime string) {
	parsedTime, err := time.ParseDuration(pxTime)
	if err != nil {
		p(err)
	}
	time.Sleep(parsedTime)
	delete(mapName, key)
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
	file.Write([]byte{typeDB})
	file.Write([]byte{byte(dbIndex)})
	//write key-vals to db
	writeKeyValue(file)

	//Write end-of-file
	file.Write([]byte{eofMarker})
	p(data)
	fmt.Println("RDB written successfully!")

}
func writeKeyValue(file *os.File) {

	for k, v := range data {
		// write type
		file.Write([]byte{typeString})
		writer(file, k)
		writer(file, v)

		// klen := len(k)
		// vlen := len(v)
		// if klen < 64 && vlen < 64 {
		// 	file.Write([]byte{byte(klen)})
		// 	// write key
		// 	file.WriteString(k)
		// 	// write vlen
		// 	file.Write([]byte{byte(vlen)})
		// 	// write val
		// 	file.WriteString(v)
		// } else if klen < 16384 && vlen < 16384 {
		// 	highByte := (length / 256) | 0x40
		// 	lowByte := length % 256
		// 	lengthBytes := []byte{byte(highByte), byte(lowByte)}
		// 	_, err := file.Write(lengthBytes)
		// 	if err != nil {
		// 		return err
		// 	}
		// } else {
		// }
	}
}

func writer(file *os.File, str string) {

	strLen := len(str)

	if strLen < 64 {
		//write len
		file.Write([]byte{byte(strLen)})
	} else if strLen < 16384 {
		highByte := (strLen / 256) | 0x40
		lowByte := strLen % 256
		strLenBytes := []byte{byte(highByte), byte(lowByte)}
		//write strlenbytes
		file.Write(strLenBytes)
	} else {
		//invalid len
		p("Invalid len: ", strLen)
	}
	//write str
	file.WriteString(str)
}

// func writeKeyValue(file *os.File, key, value string) error {
// 	file.Write([]byte{0x00})
// 	if err := writeLength(file, len(key)); err != nil {
// 		return err
// 	}
// 	_, err := file.WriteString(key)
// 	if err != nil {
// 		return err
// 	}
// 	if err := writeLength(file, len(value)); err != nil {
// 		return err
// 	}
// 	_, err = file.WriteString(value)
// 	return err
// }

// func writeLength(file *os.File, length int) error {
// 	if length < (1 << 6) {
// 		// Single-byte encoding for small lengths
// 		_, err := file.Write([]byte{byte(length)})
// 		return err
// 	} else if length < (1 << 14) {
// 		// Two-byte encoding for medium lengths
// 		val := (length & 0x3FFF) | 0x4000
// 		_, err := file.Write([]byte{byte(val >> 8), byte(val)})
// 		return err
// 	} else {
// 		// Four-byte encoding for larger lengths
// 		_, err := file.Write([]byte{0x80, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
// 		return err
// 	}
// }

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

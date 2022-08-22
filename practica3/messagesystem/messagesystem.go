package messagesystem

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"strconv"

	"github.com/DistributedClocks/GoVector/govec"
)

//Message : contenido msg
type Message struct {
	Operation string
	ID        int
}

//MessageSystem : contenido sistemma encargado de los msg
type MessageSystem struct {
	mbox   chan Message
	peers  []string
	done   chan bool
	me     int
	logger *govec.GoLog
}

//Tipos de respuesta y constantes varias
const (
	COORDINATOR = "COORDINATOR"
	ELECTION    = "ELECTION"
	LEADERBEAT  = "LEADERBEAT"
	OK          = "OK"
	MAXMESSAGES = 10000
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func parsePeers(path string) (lines []string) {
	file, err := os.Open(path)
	checkError(err)

	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return lines
}

//Send : funcion de envio de msg
func (ms MessageSystem) Send(pid int, msg Message) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", ms.peers[pid-1])
	checkError(err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)
	buf := ms.logger.PrepareSend("Sending Message"+msg.Operation, msg, govec.GetDefaultLogOptions())
	_, errWrite := conn.Write(buf)
	checkError(errWrite)
	conn.Close()
}

//Receive : funcion de recepcion de msg
func (ms *MessageSystem) Receive() (msg Message) {
	msg = <-ms.mbox
	return msg
}

//New : constructor de la clase MessageSystem
func New(whoIam int, usersFile string) (ms MessageSystem) {
	ms.me = whoIam
	ms.peers = parsePeers(usersFile)
	ms.mbox = make(chan Message, MAXMESSAGES)
	ms.done = make(chan bool)
	ms.logger = govec.InitGoVector(("Proceso_" + strconv.Itoa(whoIam)), ("Logger_" + strconv.Itoa(whoIam)), govec.GetDefaultConfig())
	go func() {
		listener, err := net.Listen("tcp", ms.peers[ms.me-1])
		fmt.Println(ms.peers[ms.me-1])
		checkError(err)
		defer close(ms.mbox)
		for {
			select {
			case <-ms.done:
				return
			default:
				conn, err := listener.Accept()
				checkError(err)
				buf := make([]byte, 512)
				var msg Message
				n, errRead := conn.Read(buf)
				checkError(errRead)
				ms.logger.UnpackReceive("Receiving Message", buf[0:n], &msg, govec.GetDefaultLogOptions())
				conn.Close()
				ms.mbox <- msg
			}
		}
	}()
	return ms
}

//Stop : funcion de stop
func (ms *MessageSystem) Stop() {
	ms.done <- true
}

/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: ricart-agrawala.go
* DESCRIPCIÓN: Implementación del algoritmo de Ricart-Agrawala Generalizado en Go
 */
package ra

import (
	shareddb "practica2/SharedDB"
	"practica2/ms"
	"strconv"
	"sync"
	"time"

	"github.com/DistributedClocks/GoVector/govec"
)

type Request struct {
	Operation string
	Clock     int
	Pid       int
	Logger    []byte
}

type Reply struct {
	Pid    int
	Logger []byte
}

type CopyDB struct {
	Pid    int
	Logger []byte
	Frase  string
}

type RASharedDB struct {
	OurSeqNum int
	HigSeqNum int
	OutRepCnt int
	ReqCS     bool
	RepDefd   []bool
	ms        *ms.MessageSystem
	done      chan bool
	chrep     chan bool
	exclude   [2][2]bool
	Mutex     sync.Mutex // mutex para proteger concurrencia sobre las variables
	me        int
	nNodes    int
	rol       string
}

var (
	logger   *govec.GoLog
	filename *shareddb.FileDB
	sCritica = make(chan bool)
)

func New(me int, nNodes int, rol string, usersFile string, flname *shareddb.FileDB) *RASharedDB {
	filename = flname
	messageTypes := []ms.Message{Request{}, Reply{}, CopyDB{}}
	msgs := ms.New(me, usersFile, messageTypes)
	matrix := [2][2]bool{
		{false, true},
		{true, true},
	}
	logger = govec.InitGoVector(("Proceso_" + strconv.Itoa(me)), ("Logger_" + strconv.Itoa(me)), govec.GetDefaultConfig())
	ra := RASharedDB{0, 0, 0, false, make([]bool, nNodes), &msgs, make(chan bool), make(chan bool), matrix, sync.Mutex{}, me, nNodes, rol}
	time.Sleep(time.Second)
	go ra.esperarMensaje()
	return &ra
}

//Pre: Verdad
//Post: Realiza  el  PreProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol() {
	ra.Mutex.Lock()
	//fmt.Println("mutexlock")
	ra.ReqCS = true
	ra.OurSeqNum = ra.HigSeqNum + 1
	ra.Mutex.Unlock()
	//fmt.Println("mutexunlock")
	ra.OutRepCnt = ra.nNodes - 1
	go ra.sendMessage()

	<-sCritica
}

//Notify all
func (ra *RASharedDB) sendMessage() {
	for count := 1; count <= ra.nNodes; count++ {
		if count != ra.me {
			aux := logShiv(Request{Operation: ra.rol, Pid: ra.me, Clock: ra.OurSeqNum})
			ra.ms.Send(count, Request{Operation: ra.rol, Pid: ra.me, Clock: ra.OurSeqNum, Logger: aux})
		}

	}
}

func (ra *RASharedDB) CopyDB() {
	for count := 1; count <= ra.nNodes; count++ {
		if count != ra.me {
			txt := ("escritura de " + strconv.Itoa(ra.me) + "\n")
			aux := logShiv(CopyDB{Pid: ra.me, Frase: txt})
			ra.ms.Send(count, CopyDB{Logger: aux, Pid: ra.me, Frase: txt})
		}

	}
}

//Pre: Verdad
//Post: Realiza  el  PostProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol() {
	ra.ReqCS = false
	for j := 0; j < ra.nNodes; j++ {
		if ra.RepDefd[j] {
			ra.RepDefd[j] = false
			aux := logShiv(Reply{Pid: ra.me})
			ra.ms.Send(j+1, Reply{Logger: aux, Pid: ra.me})
		}
	}
}

func (ra *RASharedDB) esperarMensaje() {
	for {
		m := ra.ms.Receive()
		logShivRCV(m)
		//fmt.Println("msg recibido")
		switch m.(type) {
		case Request:
			//fmt.Println("Request")
			ra.HigSeqNum = maximum(ra.HigSeqNum, m.(Request).Clock)
			ra.Mutex.Lock()
			deferIt := ra.ReqCS && ((m.(Request).Clock > ra.OurSeqNum) || (m.(Request).Clock == ra.OurSeqNum && m.(Request).Pid > ra.me)) && ra.exclude[transOperation(m.(Request).Operation)][transOperation(ra.rol)]
			ra.Mutex.Unlock()
			if deferIt {
				//fmt.Println("defer")
				ra.RepDefd[m.(Request).Pid-1] = true

			} else {
				//fmt.Println("nodefer")
				aux := logShiv(Reply{Pid: ra.me})
				ra.ms.Send(m.(Request).Pid, Reply{Logger: aux, Pid: ra.me})
			}

		case Reply:
			//fmt.Println("Reply")
			ra.OutRepCnt--
			if ra.OutRepCnt == 0 {
				sCritica <- true
			}

		case CopyDB:
			filename.EscribirFichero(m.(CopyDB).Frase)
		}
	}
}

func maximum(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func transOperation(a string) int {
	if a == "READ" {
		return 0
	}
	return 1
}

func (ra *RASharedDB) Stop() {
	ra.ms.Stop()
	ra.done <- true
}

func logShiv(msg ms.Message) []byte {
	var cabecera string
	switch msg.(type) {
	case Request:
		if msg.(Request).Operation == "READ" {
			cabecera = "Sending Message READ"
		} else {
			cabecera = "Sending Message WRITE"
		}

	case Reply:
		cabecera = "Sending Message reply"
	case CopyDB:
		cabecera = "Sending Message toall"
	}
	buf := logger.PrepareSend(cabecera, msg, govec.GetDefaultLogOptions())
	return buf
}

func logShivRCV(msg ms.Message) {
	var loger []byte
	var iface ms.Message
	switch msg.(type) {
	case Request:
		loger = msg.(Request).Logger
	case Reply:
		loger = msg.(Reply).Logger
	case CopyDB:
		loger = msg.(CopyDB).Logger
	}
	logger.UnpackReceive("Receiving Message", loger, &iface, govec.GetDefaultLogOptions())
}

/*
# AUTORES: 	Alejandro Terrón Alvarez y Oscar Anadon
# NIAs: 	761069 760628
# FICHERO: 	bully.go
# FECHA: 	19-noviembre-2021
# TIEMPO: 	2 h
# DESCRIPCION: este fichero contiene la implementación del algoritmo del maton (bully)
*/

package bully

import (
	"fmt"
	"math/rand"
	"time"

	ms "practica3/messagesystem"
)

var (
	mutexCoord  = make(chan bool, 1)
	mutexElec   = make(chan bool, 1)
	mutexnNodes = make(chan bool, 1)
)

type Bully struct {
	myId          int
	coordinatorId int
	estadoElec    bool
	nNodos        int
	channel       ms.MessageSystem

	Okchannel         chan int
	coordinatorCh     chan int
	electionChannel   chan int
	leaderBeatChannel chan int
}

func NewBully(id int, nNodes int, channel ms.MessageSystem) Bully {
	mutexCoord <- true
	mutexElec <- true
	mutexnNodes <- true
	return Bully{
		myId:              id,
		coordinatorId:     nNodes,
		estadoElec:        false,
		nNodos:            nNodes,
		channel:           channel,
		Okchannel:         make(chan int, 1),
		coordinatorCh:     make(chan int, 1),
		electionChannel:   make(chan int),
		leaderBeatChannel: make(chan int),
	}
	//TODO Mirar limites de canales
}

func (b *Bully) reception() {
	for {
		msg := b.channel.Receive()
		switch msg.Operation {
		case "OK": // recibido un ok, hay procesos con ID mayor
			fmt.Println("ok recibido")
			select {
			case b.Okchannel <- msg.ID:
				continue
			case <-time.After(1 * time.Second):
				continue // si ya no hay nadie escuchando, descarto
			}
		case "COORDINATOR": // nuevo coordinador
			fmt.Println("coordinaDOR recibido")
			b.coordinatorCh <- msg.ID
		case "ELECTION": // nuevo proceso de elección
			fmt.Println("eleccion recibido")
			b.electionChannel <- msg.ID
		case "LEADERBEAT": // el leader contesta al latido
			b.leaderBeatChannel <- msg.ID
		}
	}
}

//Init : funcion init del objeto bully
func (b *Bully) Init() {
	time.Sleep(2 * time.Second)
	go b.reception()
	go b.controlLatido()
	go b.responderElec()
	go b.nuevoLider()
	for {
	}
}

func (b *Bully) getCoordinador() int {
	<-mutexCoord
	idAux := b.coordinatorId
	mutexCoord <- true
	return idAux
}

func (b *Bully) setCoordinador(id int) {
	<-mutexCoord
	b.coordinatorId = id
	mutexCoord <- true
}

func (b *Bully) getElection() bool {
	<-mutexElec
	aux := b.estadoElec
	mutexElec <- true
	return aux
}

func (b *Bully) setElection(n bool) {
	<-mutexElec
	b.estadoElec = n
	mutexElec <- true
}

func (b *Bully) getnNodes() int {
	<-mutexnNodes
	aux := b.nNodos
	mutexnNodes <- true
	return aux
}

func (b *Bully) setnNodes(n int) {
	<-mutexnNodes
	b.nNodos = n
	mutexnNodes <- true
}

func (b *Bully) controlLatido() {
	for {
		time.Sleep(500 * time.Millisecond)
		if b.myId != b.getCoordinador() {
			timeout := rand.Intn(1000-700) + 700
			select {
			case <-b.leaderBeatChannel:
				continue
			case <-time.After(time.Duration(timeout) * time.Millisecond):
				fmt.Println("me doy cuenta yo")
				b.initElec()
			}
		} else {
			for i := 1; i <= b.nNodos; i++ {
				if i != b.myId {
					b.channel.Send(i, ms.Message{
						Operation: ms.LEADERBEAT,
						ID:        b.myId,
					})
				}
			}
		}
	}
}

func (b *Bully) responderElec() {
	for {
		id := <-b.electionChannel
		fmt.Print("RESPONDO")
		fmt.Println(id)
		b.channel.Send(id, ms.Message{
			Operation: ms.OK,
			ID:        b.myId,
		})
		b.initElec()
	}
}

func (b *Bully) initElec() {
	estado := b.getElection()
	if !estado {
		fmt.Println("entro eleccion")
		b.setElection(true)
		aux := b.getnNodes()
		b.setnNodes(aux - 1)
		fmt.Println(aux)
		if b.myId == b.getnNodes() {
			fmt.Printf("tengo la mayor id")
			b.nuevoCoordinador()
		} else {
			for i := b.myId + 1; i < aux; i++ {
				fmt.Print("envio aviso eleccion a ")
				fmt.Println(i)
				b.channel.Send(i, ms.Message{
					Operation: ms.ELECTION,
					ID:        b.myId,
				})
			}
			select {
			case <-b.Okchannel:
				idCord := <-b.coordinatorCh
				fmt.Println(idCord)
				b.setCoordinador(idCord)
				b.setnNodes(idCord)
				fmt.Println("set nNodes")
				b.setElection(false)
			case <-time.After(1 * time.Second):
				b.nuevoCoordinador()
			}
		}
	}
}

func (b *Bully) nuevoLider() {
	for {
		idCord := <-b.coordinatorCh
		fmt.Println(idCord)
		b.setCoordinador(idCord)
		b.setnNodes(idCord)
	}
}

func (b *Bully) nuevoCoordinador() {
	b.setCoordinador(b.myId)
	b.setElection(false)
	for i := 1; i < b.myId; i++ {
		if i != b.myId {
			fmt.Println("aviso de que soy lider")
			b.channel.Send(i, ms.Message{
				Operation: ms.COORDINATOR,
				ID:        b.myId,
			})
		}
	}
}

//EsperarLider funcion que fuerza una espera activa
func (b *Bully) EsperarLider() {
	time.Sleep(4 * time.Second)
	for b.myId != b.getCoordinador() {

	}
}

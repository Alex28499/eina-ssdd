package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
)

/*=====================================
-----------------LOGGER----------------
======================================*/

const (
	kEnableDebugLogs = true
	kLogToStdout     = false
	kLogOutputDir    = "./logs_raft/"
)

func (nr *NodoRaft) iniciarLogger(yo string) {
	if kEnableDebugLogs {
		nombreNodo := yo
		logPrefix := fmt.Sprintf("%s ", nombreNodo)
		if kLogToStdout {
			nr.logger = log.New(os.Stdout, nombreNodo,
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		nr.logger.Println("logger initialized")
	} else {
		nr.logger = log.New(ioutil.Discard, "", 0)
	}
}

/*=====================================
-----------------TYPE----------------
======================================*/

type AplicaOperacion struct {
	Indice int
	Op     Operacion
}

type Operacion struct {
	Clave int
	Valor string
}

type ReplyEstado struct {
	Yo    int
	Term  int
	Lider bool
}

type SometerOperacionResponse struct {
	Indice  int
	Mandato int
	EsLider bool
}

type AppendEntriesPeticion struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []AplicaOperacion
	LeaderCommit int
}

type AppendEntriesResponse struct {
	SeguidorId int
	Term       int
	Success    bool
}

type ArgsPeticionVoto struct { //Finished
	Term         int //Cadidate term
	CadidateId   int //Cadidate requesting vote
	LastLogIndex int //idx cadidadte last log
	LastLogTerm  int //term Candidato last term
}

type RespuestaPeticionVoto struct { //Finished
	Term        int  //currentTerm for candiodate to update
	VoteGranded bool //True=received vote
}

type NodoRaft struct {
	mux sync.Mutex // Mutex para proteger acceso a estado compartido

	nodos  []string // Conexiones RPC a todos los nodos (réplicas) Raft
	yo     int      // this peer's index into peers[]
	logger *log.Logger

	estado int

	canalLat   chan bool
	replyVChan chan RespuestaPeticionVoto
	appendChan chan AppendEntriesResponse

	//Pesistent state
	currentTerm int //lastest term server has seen
	votedFor    int //cadidateId that received vote in current term
	log         []AplicaOperacion

	//Volatile state
	commitIndex int //idx highest log entry known
	lastApplied int //idx highest log entry applied

	//Volatile state leader
	nextIndex  []int //idx of the next entry log to send
	matchIndex []int //idx higthest log entry replicated

}

/*=====================================
-----------------CORE------------------
======================================*/

const (
	Seguidor  = 0
	Candidato = 1
	Lider     = 2
)

func NuevoNodo(nodos []string, yo int, canalAplicar chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.nodos = nodos
	nr.yo = yo
	nr.estado = Seguidor
	nr.canalLat = make(chan bool)
	nr.replyVChan = make(chan RespuestaPeticionVoto, len(nr.nodos)-1)
	nr.appendChan = make(chan AppendEntriesResponse, len(nr.nodos)-1)
	nr.nextIndex = make([]int, len(nr.nodos))
	nr.matchIndex = make([]int, len(nr.nodos))
	nr.lastApplied = -1
	nr.votedFor = -1
	nr.commitIndex = -1
	nr.iniciarLogger(nodos[yo])
	go nr.gestionarSeguidor()
	return nr
}

func (nr *NodoRaft) estadoActual() int {
	nr.mux.Lock()
	aux := nr.estado
	nr.logger.Println("Obteniendo estado: " + strconv.Itoa(aux))
	nr.mux.Unlock()
	return aux
}

func (nr *NodoRaft) gestionarSeguidor() {
	nr.logger.Println("gestionarSeguidor()")
	rand.Seed(time.Now().UnixNano())
	timeout := time.Duration(rand.Intn(150)+150) * time.Millisecond
	for {
		select {
		case <-nr.canalLat:
		case <-time.After(timeout):
			go nr.gestionCambioEstado(Candidato)
			return
		}
	}
}

func (nr *NodoRaft) gestionarCandidato() {
	rand.Seed(time.Now().UnixNano())
	timeout := time.Duration(rand.Intn(150)+150) * time.Millisecond
	for {
		select {
		case <-time.After(timeout):
			if nr.estadoActual() == Candidato {
				nr.iniciarElecion()
			} else {
				return
			}

		}
	}
}

func (nr *NodoRaft) gestionLider() {
	nr.logger.Println("Gestion lider")
	nr.mux.Lock()
	nr.estado = Lider
	for i := 0; i < len(nr.nodos); i++ {
		nr.nextIndex[i] = len(nr.log)
		nr.matchIndex[i] = -1
	}
	nr.mux.Unlock()
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			nr.AppendEntries()
		}
	}
}

func (nr *NodoRaft) SometerOperacion(operacion Operacion, reply *SometerOperacionResponse) error {
	nr.logger.Println("sometiendo op")
	nr.mux.Lock()
	reply.Indice = nr.commitIndex + 1
	reply.Mandato = nr.currentTerm
	reply.EsLider = false
	if nr.estado == Lider {
		nr.logger.Println("Soy leader")
		nr.log = append(nr.log, AplicaOperacion{nr.currentTerm, operacion})
		nr.logger.Println("Operacion sometida")
		reply.EsLider = true
	}
	nr.mux.Unlock()
	return nil
}

func (nr *NodoRaft) gestionCambioEstado(nuevoEstado int) {
	switch nuevoEstado {
	case Seguidor:
		nr.estado = Seguidor
		nr.votedFor = -1
		go nr.gestionarSeguidor()
	case Candidato:
		nr.currentTerm++
		nr.votedFor = nr.yo
		nr.estado = Candidato
		go nr.gestionarCandidato()
	case Lider:
		nr.estado = Lider
		go nr.gestionLider()
	}
}

func (nr *NodoRaft) callAppendEntry(nodo int, request *AppendEntriesPeticion) {
	var respuesta AppendEntriesResponse
	cliente, err := rpc.Dial("tcp", nr.nodos[nodo])
	check.CheckError(err, "Error en conexion TCP")
	if cliente != nil {
		err = rpctimeout.CallTimeout(cliente, "NodoRaft.AppendEntry", &request, &respuesta, 25*time.Millisecond)
		if err == nil {
			nr.appendChan <- *&respuesta
		}
		cliente.Close()
	}

}

func (nr *NodoRaft) AppendEntries() {
	newNextIdx := len(nr.log)
	newMatchIdx := len(nr.log) - 1
	for i := 0; i < len(nr.nodos); i++ {
		if i != nr.yo {
			nr.mux.Lock()
			req := AppendEntriesPeticion{
				Term:         nr.currentTerm,
				LeaderId:     nr.yo,
				PrevLogIndex: nr.nextIndex[i],
				Entries:      nr.log[nr.nextIndex[i]:],
				LeaderCommit: nr.commitIndex,
			}
			nr.mux.Unlock()
			go nr.callAppendEntry(i, &req)
		}

	}
	for i := len(nr.nodos); i >= 0; i-- {
		select {
		case reply := <-nr.appendChan:
			nr.mux.Lock()
			if reply.Success {
				nr.nextIndex[reply.SeguidorId] = newNextIdx
				nr.matchIndex[reply.SeguidorId] = newMatchIdx

			} else {
				if nr.nextIndex[reply.SeguidorId] != 0 {
					nr.nextIndex[reply.SeguidorId]--
				}
			}
			if reply.Term > nr.currentTerm {
				nr.currentTerm = reply.Term
				nr.estado = Seguidor
			}
			nr.mux.Unlock()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (nr *NodoRaft) iniciarElecion() {
	nr.logger.Println("iniciarElecion()")
	nr.mux.Lock()
	elections_term := nr.currentTerm
	lastLogIndex := len(nr.log) - 1
	candidateLastLogTerm := -1
	if lastLogIndex > 0 {
		candidateLastLogTerm = nr.log[lastLogIndex].Indice
	}
	args := &ArgsPeticionVoto{
		Term:         nr.currentTerm,
		CadidateId:   nr.yo,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  candidateLastLogTerm,
	}
	nr.mux.Unlock()
	votosConfirmados := 1
	for i := 0; i < len(nr.nodos); i++ {
		if i != nr.yo {
			reply := &RespuestaPeticionVoto{}
			if nr.enviarPeticionVoto(i, args, reply) {
				nr.mux.Lock()
				defer nr.mux.Unlock()
				if nr.estado == Candidato && elections_term == nr.currentTerm {
					if reply.VoteGranded {
						votosConfirmados++
					}
					if votosConfirmados > len(nr.nodos)/2 {
						go nr.gestionCambioEstado(Lider)
					}
				}
			}
		}
	}
}

func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {
	nr.logger.Panicln("pidiendo voto a", nodo)
	respuesta := false
	node, err := rpc.Dial("tcp", nr.nodos[nodo])
	check.CheckError(err, "error estableciendo TCP")
	if node != nil {
		ok := rpctimeout.CallTimeout(node, "NodoRaft.PedirVoto", &args, &reply, 50*time.Millisecond)
		nr.logger.Println("me responde ", ok)
		if ok == nil {
			respuesta = true
		}
		node.Close()
	}
	return respuesta
}

/*=====================================
-----------------RPC-------------------
======================================*/

func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) error {
	nr.logger.Println("Me piden voto ", args)
	nr.mux.Lock()
	defer nr.mux.Unlock()
	reply.Term = nr.currentTerm
	if nr.currentTerm < args.Term {
		nr.currentTerm = args.Term
		lastLogIndex := len(nr.log) - 1
		if nr.estado == Seguidor {
			nr.canalLat <- true
		} else {
			go nr.gestionCambioEstado(Seguidor)
			return nil
		}
		if nr.currentTerm == args.Term &&
			(nr.votedFor == -1 || nr.votedFor == args.CadidateId) || (lastLogIndex <= 0) ||
			(args.LastLogIndex >= lastLogIndex &&
				args.LastLogTerm == nr.log[lastLogIndex].Indice) {
			reply.VoteGranded = true
			nr.votedFor = args.CadidateId
		}
	}
	return nil

}

func (nr *NodoRaft) AppendEntry(args *AppendEntriesPeticion, reply *AppendEntriesResponse) error {
	nr.mux.Lock()
	defer nr.mux.Unlock()
	if args.Term > nr.currentTerm {
		nr.currentTerm = args.Term
	}
	if nr.estado == Seguidor {
		nr.canalLat <- true
	}
	if (nr.currentTerm >= args.Term && nr.estado == Candidato) || nr.currentTerm > args.Term {
		reply.Success = false

	} else {
		reply.Success = true
	}
	return nil

}

func (nr *NodoRaft) Para(_, _ *struct{}) error {
	nr.logger.Println("Para()")
	go func() {
		time.Sleep(100 + time.Millisecond)
		os.Exit(0)
	}()
	return nil
}

func (nr *NodoRaft) ObtenerEstado(_, reply *ReplyEstado) error {
	nr.logger.Println("ObtenerEstado()")
	nr.mux.Lock()
	reply.Yo = nr.yo
	reply.Term = nr.currentTerm
	reply.Lider = false
	if nr.estado == Lider {
		reply.Lider = true
	}
	nr.mux.Unlock()
	return nil
}

package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"raft/internal/comun/rpctimeout"
)

/* -----------------------------------------------------------------------------
									LOGGER
------------------------------------------------------------------------------*/

const (
	IntNOINICIALIZADO = -1
	// If true, enables depuration logging.
	kEnableDebugLogs = true
	// If true, logging output to stdout, if not, to kLogOutputDir file.
	kLogToStdout = false
	// Logging file name.
	kLogOutputDir = "./logs_raft/"
)

// Returns new raft Logger.
func (nr *NodoRaft) inicializarLogger(nodo rpctimeout.HostPort) {
	if kEnableDebugLogs {
		nombreNodo := nodo.Host() + "_" + nodo.Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
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
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}
}

/* -----------------------------------------------------------------------------
									TYPERS
------------------------------------------------------------------------------*/

type Vacio struct{}

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

type EstadoRemoto struct {
	IdNodo  int
	Mandato int
	EsLider bool
	IdLider int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type ArgsPeticionVoto struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	Mandato        int
	EsLider        bool
	IdLider        int
}

type RespPeticionVoto struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []AplicaOperacion
	LeaderCommit int
}

type AppendEntriesResp struct {
	Term    int
	Success bool
	Xterm   int
	XIndex  int
	XLen    int
}

type NodoRaft struct {
	Mux     sync.Mutex
	Nodos   []rpctimeout.HostPort
	Yo      int
	IdLider int

	Logger *log.Logger

	Estado int

	//Pesistent state
	CurrentTerm int
	VotedFor    int
	log         []AplicaOperacion

	//Volatile state
	CommitIndex  int
	LastApplied  int
	LastLogIndex int
	//Volatile state leader
	NextIndex    []int
	MatchIndex   []int
	lastAccessed time.Time
}

const (
	Seguidor = iota
	Candidato
	Lider
)

const MinElectionTimeout = 150
const MaxElectionTimeout = 300

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randTimeout() time.Duration {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	randTimeout := MinElectionTimeout + r1.Intn(MaxElectionTimeout-MinElectionTimeout)
	return time.Duration(randTimeout) * time.Millisecond
}

func (nr *NodoRaft) enviarPeticionVoto(server int, argsVoto *ArgsPeticionVoto, respVoto *RespPeticionVoto) bool {
	ok := nr.Nodos[server].CallTimeout("NodoRaft.PedirVoto", argsVoto, &respVoto, 30*time.Millisecond)
	nr.Logger.Println("RequestVote to ", server, ok)
	return ok == nil
}

func (nr *NodoRaft) sendAppendEntries(server int, argsLat *AppendEntriesArgs, respLat *AppendEntriesResp) bool {
	ok := nr.Nodos[server].CallTimeout("NodoRaft.AppendEntries", argsLat, respLat, 30*time.Millisecond)
	return ok == nil
}

func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	nr.Mux.Lock()
	yo := nr.Yo
	mandato := nr.CurrentTerm
	idLider := nr.IdLider
	esLider := false
	if nr.Estado == Lider {
		esLider = true
	}
	nr.Mux.Unlock()
	return yo, mandato, esLider, idLider
}

func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	indice := IntNOINICIALIZADO
	mandato := IntNOINICIALIZADO
	EsLider := false
	idLider := nr.IdLider
	valorADevolver := ""
	nr.Mux.Lock()
	if nr.Estado == Lider {
		nr.log = append(nr.log, AplicaOperacion{nr.CurrentTerm, operacion})
		fmt.Println(nr.log)
		indice = nr.NextIndex[nr.Yo]
		mandato = nr.CurrentTerm
		EsLider = true
		for i := range nr.Nodos {
			if i != nr.Yo {
				//go nr.submit(i)
			}
		}
		idLider = IntNOINICIALIZADO
	}
	nr.Mux.Unlock()
	return indice, mandato, EsLider, idLider, valorADevolver
}

func (nr *NodoRaft) GestionarNodoRaft() {
	for true {
		nr.Mux.Lock()
		EstadoActual := nr.Estado
		nr.Mux.Unlock()
		switch EstadoActual {
		case Seguidor:
			nr.GestionarSeguidor()
		case Candidato:
			nr.GestionarCandidato()
		case Lider:
			nr.GestionarLider()
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (nr *NodoRaft) GestionarSeguidor() {
	nr.Logger.Println("Soy seguidor")
	duration := randTimeout()
	nr.Logger.Println("El timeout es de ", duration)
	time.Sleep(duration)
	nr.Mux.Lock()
	lastAccessed := nr.lastAccessed
	nr.Mux.Unlock()
	if time.Now().Sub(lastAccessed).Milliseconds() >= duration.Milliseconds() {
		nr.GestionarCambioEstado(Candidato)
	}
}

func (nr *NodoRaft) GestionarCambioEstado(nuevoEstado int) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	switch nuevoEstado {
	case Seguidor:
		nr.Estado = Seguidor
		nr.VotedFor = IntNOINICIALIZADO
	case Candidato:
		nr.Estado = Candidato
		nr.CurrentTerm++
		nr.VotedFor = nr.Yo
	case Lider:
		nr.Estado = Lider
		for i := 0; i < len(nr.Nodos); i++ {
			nr.NextIndex[i] = len(nr.log)
			nr.MatchIndex[i] = -1
		}
	}
}

func (nr *NodoRaft) BroadcastPeticionVoto() int {
	votos := 1
	for i := range nr.Nodos {
		if i != nr.Yo {
			nr.Mux.Lock()
			argsVoto := ArgsPeticionVoto{
				Term:         nr.CurrentTerm,
				CandidateId:  nr.Yo,
				LastLogIndex: nr.LastLogIndex,
				LastLogTerm:  nr.log[nr.LastLogIndex].Indice,
			}
			nr.Mux.Unlock()
			respVoto := RespPeticionVoto{}
			if ok := nr.enviarPeticionVoto(i, &argsVoto, &respVoto); ok {
				if respVoto.VoteGranted {
					votos++
				}
				if argsVoto.Term < respVoto.Term {
					nr.GestionarCambioEstado(Seguidor)
				}
			}
		}
	}
	nr.Logger.Println("Votos confirmados", votos)
	return votos
}

func (nr *NodoRaft) GestionarCandidato() {
	nr.Logger.Println("Soy candidato")
	votosConf := nr.BroadcastPeticionVoto()
	if nr.Estado == Candidato && votosConf >= len(nr.Nodos)/2+1 {
		nr.GestionarCambioEstado(Lider)
	} else {
		nr.GestionarCambioEstado(Seguidor)
	}
}

func (nr *NodoRaft) GestionarLider() {
	nr.Logger.Println("Soy lider")
	nr.Mux.Lock()
	me := nr.Yo
	term := nr.CurrentTerm
	commitIndex := nr.CommitIndex
	peers := nr.Nodos
	nextIndex := nr.NextIndex

	lastLogIndex := nr.LastLogIndex
	matchIndex := nr.MatchIndex
	nextIndex[me] = lastLogIndex + 1
	matchIndex[me] = lastLogIndex
	log := nr.log
	nr.Mux.Unlock()
	for n := commitIndex + 1; n <= lastLogIndex; n++ {
		count := 0
		total := len(peers)
		majority := (total / 2) + 1
		for peer := range peers {
			if matchIndex[peer] >= n && log[n].Indice == term {
				count++
			}
		}

		if count >= majority {
			nr.Mux.Lock()
			i := nr.CommitIndex + 1
			for ; i <= n; i++ {
				nr.CommitIndex = nr.CommitIndex + 1
			}
			nr.Mux.Unlock()
		}
	}

	for peer := range peers {
		if peer == me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesResp{}
		nr.Mux.Lock()
		args.Term = nr.CurrentTerm
		prevLogIndex := nextIndex[peer] - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = nr.log[prevLogIndex].Indice
		args.LeaderCommit = nr.CommitIndex
		args.LeaderId = nr.Yo
		if nextIndex[peer] <= lastLogIndex {
			args.Entry = nr.log[prevLogIndex+1 : lastLogIndex+1]
		}
		nr.Mux.Unlock()

		go func(peer int) {
			ok := nr.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}

			nr.Mux.Lock()
			if reply.Success {
				nr.NextIndex[peer] = min(nr.NextIndex[peer]+len(args.Entry), nr.LastLogIndex+1)
				nr.MatchIndex[peer] = prevLogIndex + len(args.Entry)
			} else {
				if reply.Term > args.Term {
					nr.Estado = Seguidor
					nr.Mux.Unlock()
					return
				}
				if reply.Xterm == -1 {
					nr.NextIndex[peer] = reply.XLen
					nr.Mux.Unlock()
					return
				}
				index := -1
				for i, v := range nr.log {
					if v.Indice == reply.Xterm {
						index = i
					}
				}
				if index == -1 {
					nr.NextIndex[peer] = reply.XIndex
				} else {
					nr.NextIndex[peer] = index
				}
			}
			nr.Mux.Unlock()
		}(peer)
	}
}

func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = IntNOINICIALIZADO
	nr.CurrentTerm = 0
	nr.VotedFor = IntNOINICIALIZADO
	nr.Estado = Seguidor
	operacion := TipoOperacion{
		Operacion: "Escritura",
		Clave:     "Inicio Log",
		Valor:     "",
	}
	nr.log = []AplicaOperacion{
		{
			Indice:    0,
			Operacion: operacion,
		},
	}
	nr.inicializarLogger(nodos[yo])
	nr.NextIndex = make([]int, len(nr.Nodos))
	nr.MatchIndex = make([]int, len(nr.Nodos))
	go nr.GestionarNodoRaft()

	return nr
}

/* -----------------------------------------------------------------------------
									RPC
------------------------------------------------------------------------------*/

func (nr *NodoRaft) ParaNodo(args Vacio, resp *Vacio) error {
	defer nr.para()
	return nil
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, resp *EstadoRemoto) error {
	resp.IdNodo, resp.Mandato, resp.EsLider, resp.IdLider = nr.obtenerEstado()
	return nil
}

func (nr *NodoRaft) AppendEntries(argsLat *AppendEntriesArgs, respLat *AppendEntriesResp) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	respLat.Xterm = -1
	respLat.XIndex = -1
	respLat.XLen = len(nr.log)
	respLat.Success = false
	respLat.Term = nr.CurrentTerm
	if argsLat.Term < nr.CurrentTerm {
		return nil
	}

	if len(nr.log) < argsLat.PrevLogIndex+1 {
		return nil
	}

	if nr.log[argsLat.PrevLogIndex].Indice != argsLat.PrevLogTerm {
		respLat.Xterm = nr.log[argsLat.PrevLogIndex].Indice
		for i, v := range nr.log {
			if v.Indice == respLat.Xterm {
				respLat.XIndex = i
				break
			}
		}
		return nil
	}

	index := 0
	for ; index < len(argsLat.Entry); index++ {
		currentIndex := argsLat.PrevLogIndex + 1 + index
		if currentIndex > len(nr.log)-1 {
			break
		}
		if nr.log[currentIndex].Indice != argsLat.Entry[index].Indice {
			nr.log = nr.log[:currentIndex]
			nr.LastLogIndex = len(nr.log) - 1
			break
		}
	}

	respLat.Success = true
	nr.lastAccessed = time.Now()
	if len(argsLat.Entry) > 0 {
		nr.log = append(nr.log, argsLat.Entry[index:]...)
		nr.LastLogIndex = len(nr.log) - 1
	}
	if argsLat.LeaderCommit > nr.CommitIndex {
		min := min(argsLat.LeaderCommit, nr.LastLogIndex)
		for i := nr.CommitIndex + 1; i <= min; i++ {
			nr.CommitIndex = i
		}
	}
	return nil
}

func (nr *NodoRaft) PedirVoto(argsVoto *ArgsPeticionVoto, respVoto *RespPeticionVoto) error {
	nr.Logger.Println("me pide voto", argsVoto)
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	if nr.CurrentTerm > argsVoto.Term || (nr.CurrentTerm == argsVoto.Term && nr.VotedFor != -1) {
		respVoto.Term, respVoto.VoteGranted = nr.CurrentTerm, false
		return nil
	}

	if nr.CurrentTerm == argsVoto.Term && nr.VotedFor == argsVoto.CandidateId {
		respVoto.VoteGranted, respVoto.Term = true, nr.CurrentTerm
		return nil
	}

	if argsVoto.Term > nr.CurrentTerm {
		nr.CurrentTerm, nr.VotedFor = argsVoto.Term, -1
		nr.Estado = Seguidor
	}

	respVoto.Term = argsVoto.Term
	if nr.LastLogIndex-1 >= 0 {
		lastLogTerm := nr.log[nr.LastLogIndex-1].Indice
		if lastLogTerm > argsVoto.LastLogTerm || (lastLogTerm == argsVoto.LastLogTerm && nr.LastLogIndex > argsVoto.LastLogIndex) {
			respVoto.VoteGranted = false
			return nil
		}
	}

	nr.Estado = Seguidor
	respVoto.VoteGranted = true
	nr.VotedFor = argsVoto.CandidateId
	nr.lastAccessed = time.Now()
	return nil
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	resp *ResultadoRemoto) error {
	resp.IndiceRegistro, resp.Mandato, resp.EsLider,
		resp.IdLider, resp.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

func (nr *NodoRaft) CheckCommits(args int, resp *bool) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	*resp = false
	if nr.CommitIndex == args {
		*resp = true
	}
	return nil
}

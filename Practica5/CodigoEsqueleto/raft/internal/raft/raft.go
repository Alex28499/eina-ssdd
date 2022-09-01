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
									TYPES
------------------------------------------------------------------------------*/

type Vacio struct{}

type TipoOperacion struct {
	Operacion string
	Clave     string
	Valor     string
}

type AplicaOperacion struct {
	Indice    int
	Operacion TipoOperacion
}

type EstadoRemoto struct {
	IdNodo  int
	Mandato int
	EsLider bool
	IdLider int
}

type ArgsPeticionVoto struct {
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
	ok := nr.Nodos[server].CallTimeout("NodoRaft.PedirVoto", argsVoto, &respVoto, 50*time.Millisecond)
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
		indice = nr.NextIndex[nr.Yo]
		mandato = nr.CurrentTerm
		EsLider = true
		go nr.BroadcastNuevaOperacion(operacion)
	}
	nr.Mux.Unlock()
	return indice, mandato, EsLider, idLider, valorADevolver
}

func (nr *NodoRaft) BroadcastNuevaOperacion(operacion TipoOperacion) {
	for nodo := range nr.Nodos {
		if nodo != nr.Yo {
			nr.Mux.Lock()
			nextIdx := nr.NextIndex[nodo]
			nr.Mux.Unlock()
			for {
				args := nr.crearLatOperacion(nodo)
				done := nr.enviarNuevaOperacion(nodo, args, nextIdx)
				if done {
					break
				}
			}
		}
	}
}

func (nr *NodoRaft) enviarNuevaOperacion(nodo int, argsLat AppendEntriesArgs, nextIdx int) bool {
	var done = false
	var reply AppendEntriesResp
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", argsLat, &reply, 40*time.Millisecond)
	if err == nil {
		nr.Mux.Lock()
		if reply.Success {
			done = true
			nr.NextIndex[nodo] = nextIdx + len(argsLat.Entry)
			nr.MatchIndex[nodo] = nr.NextIndex[nodo] - 1
			index := nr.CommitIndex + 1
			match := true
			for index < len(nr.log) && match {
				if nr.log[index].Indice == nr.CurrentTerm {
					matchLog := 1
					for j := range nr.Nodos {
						if nr.MatchIndex[j] >= index {
							matchLog++
						}
					}
					if matchLog*2 >= len(nr.Nodos)+1 {
						nr.CommitIndex = index
					} else {
						match = false
					}
				}
				index++
			}
		} else {
			if nr.NextIndex[nodo] > 0 {
				nr.NextIndex[nodo] = nr.NextIndex[nodo] - 1
			}
		}
		nr.Mux.Unlock()
	}
	return done
}

func (nr *NodoRaft) crearLatOperacion(i int) AppendEntriesArgs {
	nr.Mux.Lock()
	prevLogIndex := -1
	prevLogTerm := -1
	nrIndex := nr.NextIndex[i]
	if nrIndex-1 >= 0 {
		prevLogIndex = nrIndex - 1
		prevLogTerm = nr.log[prevLogIndex].Indice
	}
	entries := nr.log[nrIndex:]
	args := AppendEntriesArgs{
		Term:         nr.CurrentTerm,
		LeaderId:     nr.Yo,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entry:        entries,
		LeaderCommit: nr.CommitIndex,
	}
	nr.Mux.Unlock()
	return args
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
	duration := randTimeout()
	time.Sleep(duration)
	nr.Mux.Lock()
	lastAccessed := nr.lastAccessed
	nr.Mux.Unlock()
	if time.Now().Sub(lastAccessed).Milliseconds() >= duration.Milliseconds() {
		nr.GestionarCambioEstado(Candidato, true)
	}
}

func (nr *NodoRaft) GestionarCambioEstado(nuevoEstado int, muxtex bool) {
	if muxtex {
		nr.Mux.Lock()
		defer nr.Mux.Unlock()
	}
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
		for nodo := range nr.Nodos {
			nr.NextIndex[nodo] = len(nr.log)
			nr.MatchIndex[nodo] = -1
		}
	}
}

func (nr *NodoRaft) BroadcastPeticionVoto() int {
	votos := 1
	for i := range nr.Nodos {
		if i != nr.Yo {
			nr.Mux.Lock()
			currentTerm := nr.CurrentTerm
			lastLogIdx := -1
			lastLogTerm := -1
			if len(nr.log) > 0 {
				lastLogIdx = len(nr.log) - 1
				lastLogTerm = nr.log[lastLogIdx].Indice
			}
			argsVoto := ArgsPeticionVoto{
				Term:         nr.CurrentTerm,
				CandidateId:  nr.Yo,
				LastLogIndex: lastLogIdx,
				LastLogTerm:  lastLogTerm,
			}
			nr.Mux.Unlock()
			respVoto := RespPeticionVoto{}
			if ok := nr.enviarPeticionVoto(i, &argsVoto, &respVoto); ok {
				if (currentTerm == respVoto.Term) && respVoto.VoteGranted {
					votos++
				}
				if currentTerm < respVoto.Term {
					nr.GestionarCambioEstado(Seguidor, true)
				}
			}
		}
	}
	return votos
}

func (nr *NodoRaft) GestionarCandidato() {
	votosConf := nr.BroadcastPeticionVoto()
	if nr.Estado == Candidato && votosConf >= len(nr.Nodos)/2+1 {
		nr.GestionarCambioEstado(Lider, true)
	} else {
		nr.GestionarCambioEstado(Seguidor, true)
	}
}

func (nr *NodoRaft) GestionarLider() {
	for nodo := range nr.Nodos {
		if nodo != nr.Yo {
			respLat := &AppendEntriesResp{}
			argsLat := AppendEntriesArgs{Term: nr.CurrentTerm, LeaderId: nr.Yo, LeaderCommit: nr.CommitIndex}
			err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", argsLat, respLat, 50*time.Millisecond)
			if err == nil {
				if !respLat.Success {
					nr.Mux.Lock()
					if nr.NextIndex[nodo] > 0 {
						nr.NextIndex[nodo] = nr.NextIndex[nodo] - 1
					}
					nr.Mux.Unlock()
				}
			}
		}
	}
}

func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.inicializarLogger(nodos[yo])
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = IntNOINICIALIZADO
	nr.CurrentTerm = 0
	nr.VotedFor = IntNOINICIALIZADO
	nr.CommitIndex = IntNOINICIALIZADO
	nr.LastApplied = IntNOINICIALIZADO
	nr.Estado = Seguidor
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

func (nr *NodoRaft) ObtenerCommitIdx(args int, coincide *bool) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	nr.Logger.Println("El log es", nr.log)
	*coincide = false
	if nr.CommitIndex+1 == args {
		*coincide = true
	}
	return nil
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	resp *ResultadoRemoto) error {
	resp.IndiceRegistro, resp.Mandato, resp.EsLider,
		resp.IdLider, resp.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

func (nr *NodoRaft) AppendEntries(argsLat *AppendEntriesArgs, respLat *AppendEntriesResp) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	if argsLat.Term > nr.CurrentTerm {
		nr.GestionarCambioEstado(Seguidor, false)
		nr.CurrentTerm = argsLat.Term
		return nil
	}
	respLat.Success = false
	if argsLat.Term == nr.CurrentTerm {
		respLat.Term = nr.CurrentTerm
		if nr.Estado != Seguidor {
			nr.GestionarCambioEstado(Seguidor, false)
			nr.CurrentTerm = argsLat.Term
			return nil
		}
		nr.lastAccessed = time.Now()
		if argsLat.PrevLogIndex == IntNOINICIALIZADO {
			respLat.Success = true
			nr.log = argsLat.Entry
			return nil
		}
		if len(nr.log) > argsLat.PrevLogIndex && nr.log[argsLat.PrevLogIndex].Indice == argsLat.PrevLogTerm {
			respLat.Success = true
			if len(argsLat.Entry) > 0 {
				nr.log = append(nr.log[:argsLat.PrevLogIndex+1], argsLat.Entry...)
			}
		}
		if argsLat.LeaderCommit > nr.CommitIndex {
			if len(nr.log) >= argsLat.LeaderCommit {
				nr.CommitIndex = min(len(nr.log)-1, argsLat.LeaderCommit)
			}
		}
	}
	return nil
}

func (nr *NodoRaft) PedirVoto(argsVoto *ArgsPeticionVoto, respVoto *RespPeticionVoto) error {
	//nr.Logger.Println("me pide voto", argsVoto)
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	lastLogIdx := -1
	lastLogTerm := -1
	if len(nr.log) > 0 {
		lastLogIdx = len(nr.log) - 1
		lastLogTerm = nr.log[lastLogIdx].Indice
	}
	if argsVoto.Term < nr.CurrentTerm {
		respVoto.VoteGranted = false
	}
	if argsVoto.Term > nr.CurrentTerm {
		nr.GestionarCambioEstado(Seguidor, false)
		nr.CurrentTerm = argsVoto.Term
	}
	if nr.CurrentTerm == argsVoto.Term && (nr.VotedFor == IntNOINICIALIZADO || nr.VotedFor == argsVoto.CandidateId) &&
		((argsVoto.LastLogIndex >= lastLogIdx && argsVoto.LastLogTerm == lastLogTerm) || argsVoto.LastLogTerm > lastLogTerm) {
		respVoto.VoteGranted = true
		nr.VotedFor = argsVoto.CandidateId
		nr.lastAccessed = time.Now()
	} else {
		respVoto.VoteGranted = false
	}
	respVoto.Term = nr.CurrentTerm
	return nil
}

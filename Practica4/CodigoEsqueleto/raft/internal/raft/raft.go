// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

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

	"raft/internal/comun/rpctimeout"
)

//  false deshabilita por completo los logs de depuracion
// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
const kEnableDebugLogs = true

// Poner a true para logear a stdout en lugar de a fichero
const kLogToStdout = false

// Cambiar esto para salida de logs en un directorio diferente
const kLogOutputDir = "./logs_raft/"

//RAFT node states
const Seguidor = 0
const Candidato = 1
const Leader = 2

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice int // en la entrada de registro
	Op     Operacion
}

type Operacion struct {
	Clave int
	Valor string
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
//
type NodoRaft struct {
	mux sync.Mutex // Mutex para proteger acceso a estado compartido

	nodos []string // Conexiones RPC a todos los nodos (réplicas) Raft
	yo    int      // this peer's index into peers[]
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	logger *log.Logger

	// Vuestros datos aqui.
	estado int

	canalLat   chan bool
	replyVChan chan RespuestaPeticionVoto
	appendChan chan AppendEntriesResponse

	// mirar figura 2 para descripción del estado que debe mantenre un nodo Raft

	//Pesistent state
	currentTerm int //lastest term server has seen
	votedFor    int //cadidateId that received vote in current term
	log         []AplicaOperacion

	//Volatile state
	comitIndex  int //idx highest log entry known
	lastApplied int //idx highest log entry applied

	//Volatile state leader
	nextIndex  []int //idx of the next entry log to send
	matchIndex []int //idx higthest log entry replicated

}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion

//TODO::CHANGES
//func NuevoNodo(nodos []*rpc.Client, yo int, canalAplicar chan AplicaOperacion) *NodoRaft {
func NuevoNodo(nodos []string, yo int, canalAplicar chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.nodos = nodos
	nr.yo = yo
	nr.estado = Seguidor
	nr.canalLat = make(chan bool)
	nr.replyVChan = make(chan RespuestaPeticionVoto, len(nr.nodos)-1)
	nr.appendChan = make(chan AppendEntriesResponse, len(nr.nodos)-1)
	nr.nextIndex = make([]int, len(nodos))
	nr.matchIndex = make([]int, len(nodos))
	nr.currentTerm = 0
	nr.votedFor = -1
	nr.comitIndex = 0
	nr.lastApplied = 0
	operacion := Operacion{Clave: 0, Valor: "start"}
	nr.log = []AplicaOperacion{
		{
			Indice: 0,
			Op:     operacion,
		},
	}
	nr.iniciarLogger(nodos[yo])
	go nr.gestionarSeguidor()
	return nr
}

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

//Funcion atomica para conocer estado actual nodo
func (nr *NodoRaft) estadoActual() int { //Finished
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
			go nr.iniciarElecion()
			return
		}
	}
}

func (nr *NodoRaft) gestionarCandidato() {
	nr.mux.Lock()
	nr.estado = Candidato
	nr.currentTerm++
	nr.votedFor = nr.yo
	nr.mux.Unlock()
}

func (nr *NodoRaft) gestionLider() {
	nr.logger.Println("Gestion lider")
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			nr.AppendEntries()
		}
	}
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) Para(_, _ *struct{}) error { //Finished
	nr.logger.Println("Para()")
	go func() {
		time.Sleep(100 + time.Millisecond)
		os.Exit(0)
	}()
	return nil
}

type ReplyEstado struct { //Finished
	// Vuestros datos aqui
	Yo    int
	Term  int
	Lider bool
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
func (nr *NodoRaft) ObtenerEstado(_, reply *ReplyEstado) error { //Finished
	nr.logger.Println("ObtenerEstado()")
	nr.mux.Lock()
	reply.Yo = nr.yo
	reply.Term = nr.currentTerm
	reply.Lider = false
	if nr.estado == Leader {
		reply.Lider = true
	}
	nr.mux.Unlock()
	return nil
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver con
// rapidez
//
// No hay garantia que esta operacion consiga comprometerse n una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
type SometerOperacionResponse struct {
	Indice  int
	Mandato int
	EsLider bool
}

func (nr *NodoRaft) SometerOperacion(operacion Operacion, reply *SometerOperacionResponse) error {
	nr.logger.Println("sometiendo op")
	nr.mux.Lock()
	reply.Indice = nr.comitIndex + 1
	reply.Mandato = nr.currentTerm
	reply.EsLider = false
	if nr.estado == Leader {
		nr.logger.Println("Soy leader")
		nr.log = append(nr.log, AplicaOperacion{nr.currentTerm, operacion})
		nr.logger.Println("Operacion sometida")
		reply.EsLider = true
	}
	nr.mux.Unlock()
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

func (nr *NodoRaft) callAppendEntry(nodo int, request *AppendEntriesPeticion) {
	var respuesta AppendEntriesResponse
	cliente, err := rpc.Dial("tcp", nr.nodos[nodo])
	checkError(err)
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
				LeaderCommit: nr.comitIndex,
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

//
// ArgsPeticionVoto
// ===============
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
type ArgsPeticionVoto struct { //Finished
	// Vuestros datos aqui
	Term         int //Cadidate term
	CadidateId   int //Cadidate requesting vote
	LastLogIndex int //idx cadidadte last log
	LastLogTerm  int //term Candidato last term
}

//
// RespuestaPeticionVoto
// ================
//
// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
//
type RespuestaPeticionVoto struct { //Finished
	// Vuestros datos aqui
	Term        int  //currentTerm for candiodate to update
	VoteGranded bool //True=received vote
}

func (nr *NodoRaft) iniciarElecion() {
	nr.logger.Println("iniciarElecion()")
	rand.Seed(time.Now().UnixNano())
	nr.gestionarCandidato()
	votosConfimados := 1
	nr.mux.Lock()
	req := &ArgsPeticionVoto{
		Term:         nr.currentTerm,
		CadidateId:   nr.yo,
		LastLogIndex: nr.lastApplied,
		LastLogTerm:  nr.log[nr.lastApplied].Indice,
	}
	nr.mux.Unlock()
	for i := 0; i < len(nr.nodos); i++ {
		if i != nr.yo {
			resp := &RespuestaPeticionVoto{}
			go nr.enviarPeticionVoto(i, req, resp)
		}
	}
	minTimeout := 300
	maxTimeout := 600
	for {
		select {
		case reply := <-nr.replyVChan:
			if reply.Term > nr.currentTerm {
				nr.logger.Panicln(reply.Term, ">", nr.currentTerm)
				nr.mux.Lock()
				nr.currentTerm = reply.Term
				nr.votedFor = -1
				nr.estado = Seguidor
				nr.mux.Unlock()
				break
			}
			if reply.VoteGranded {
				votosConfimados++
				if votosConfimados >= len(nr.nodos)/2 {
					nr.mux.Lock()
					nr.logger.Println("Soy leader ----------------------------------------------------------------------------------------------------------------->")
					nr.estado = Leader
					nr.logger.Println(nr.estado)
					for i := 0; i < len(nr.nodos); i++ {
						nr.nextIndex[i] = len(nr.log)
						nr.matchIndex[i] = -1
					}
					nr.mux.Unlock()
					nr.gestionLider()
				}

			}
			break
		case <-time.After(time.Duration(rand.Intn(maxTimeout-minTimeout)+minTimeout) * time.Millisecond):
			break
		}
	}

}

//
// PedirVoto
// ===========
//
// Metodo para RPC PedirVoto
//
func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) error {
	nr.mux.Lock()
	defer nr.mux.Unlock()
	if args.Term >= nr.currentTerm && (nr.votedFor == -1 || nr.votedFor == args.CadidateId) && args.LastLogIndex >= nr.lastApplied {
		nr.votedFor = args.CadidateId
		nr.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranded = true
		nr.estado = Seguidor
		nr.canalLat <- true

	} else {
		reply.Term = nr.currentTerm
		reply.VoteGranded = false
	}
	return nil

}

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumetnos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petiión perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) {

	node, _ := rpc.Dial("tcp", nr.nodos[nodo])
	if node != nil {
		ok := rpctimeout.CallTimeout(node, "NodoRaft.PedirVoto", &args, &reply, 150*time.Millisecond)
		if ok == nil {
			nr.replyVChan <- *reply
		}
		node.Close()
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

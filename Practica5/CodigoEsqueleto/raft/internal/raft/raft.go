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
	"os"

	//"crypto/rand"
	"sync"
	"time"

	//"net/rpc"

	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"

	//RAFT node states
	Seguidor  = 0
	Candidato = 1
	Leader    = 2
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
//
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	estado int

	canalLat   chan bool
	replyVChan chan RespuestaPeticionVoto
	appendChan chan Results

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

func (nr *NodoRaft) inicializarLogger(nodos []rpctimeout.HostPort, yo int) {
	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
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
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1

	nr.estado = Seguidor
	nr.canalLat = make(chan bool)
	nr.replyVChan = make(chan RespuestaPeticionVoto, len(nr.Nodos)-1)
	nr.appendChan = make(chan Results, len(nr.Nodos)-1)
	nr.nextIndex = make([]int, len(nodos))
	nr.matchIndex = make([]int, len(nodos))
	nr.currentTerm = 0
	nr.votedFor = -1
	nr.comitIndex = 0
	nr.lastApplied = 0
	nr.log = []AplicaOperacion{}
	nr.inicializarLogger(nr.Nodos, yo)
	go nr.gestionarSeguidor()

	return nr
}

func (nr *NodoRaft) gestionarSeguidor() {
	nr.Logger.Println("gestionarSeguidor()")
	rand.Seed(time.Now().UnixNano())
	timeout := time.Duration(rand.Intn(150)+150) * time.Millisecond
	for {
		select {
		case <-nr.canalLat:
		case <-time.After(timeout):
			go nr.iniciarEleccion()
			return
		}
	}
}

func (nr *NodoRaft) gestionarCandidato() {
	nr.Mux.Lock()
	nr.estado = Candidato
	nr.currentTerm++
	nr.votedFor = nr.Yo
	nr.Mux.Unlock()
}

func (nr *NodoRaft) gestionLider() {
	nr.Logger.Println("Gestion lider")
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			nr.generarLatido()
		}
	}
}

func (nr *NodoRaft) iniciarEleccion() {
	nr.Logger.Println("iniciarElecion()")
	rand.Seed(time.Now().UnixNano())
	nr.gestionarCandidato()
	votosConfimados := 1
	nr.Mux.Lock()
	cdLastLogterm := -1
	if nr.lastApplied > 0 {
		cdLastLogterm = nr.log[nr.lastApplied].Indice
	}
	req := &ArgsPeticionVoto{
		Term:         nr.currentTerm,
		CadidateId:   nr.Yo,
		LastLogIndex: nr.lastApplied,
		LastLogTerm:  cdLastLogterm,
	}
	nr.Mux.Unlock()
	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			resp := &RespuestaPeticionVoto{}
			go nr.enviarPeticionVoto(i, req, resp)
		}
	}
	minTimeout := 300
	maxTimeout := 600
	for {
		nr.Logger.Println("esperoRespuesta()")
		select {
		case reply := <-nr.replyVChan:
			if reply.Term > nr.currentTerm {
				nr.Logger.Panicln(reply.Term, ">", nr.currentTerm)
				nr.Mux.Lock()
				nr.currentTerm = reply.Term
				nr.votedFor = -1
				nr.estado = Seguidor
				nr.Mux.Unlock()
				break
			}
			if reply.VoteGranded {
				votosConfimados++
				if votosConfimados >= len(nr.Nodos)/2 {
					nr.Mux.Lock()
					nr.Logger.Println("Soy leader ----------------------------------------------------------------------------------------------------------------->")
					nr.estado = Leader
					nr.Logger.Println(nr.estado)
					for i := 0; i < len(nr.Nodos); i++ {
						nr.nextIndex[i] = len(nr.log)
						nr.matchIndex[i] = -1
					}
					nr.Mux.Unlock()
					nr.gestionLider()
				}

			}
			break
		case <-time.After(time.Duration(rand.Intn(maxTimeout-minTimeout)+minTimeout) * time.Millisecond):
			break
		}
	}

}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	yo := nr.Yo

	idLider := nr.IdLider
	nr.Logger.Println("El idLider es ", idLider)

	mandato := nr.currentTerm
	esLider := false
	if nr.IdLider == nr.Yo {
		esLider = true
	}

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	indice := -1
	mandato := -1
	EsLider := false
	idLider := -1
	valorADevolver := ""

	// Vuestro codigo aqui

	return indice, mandato, EsLider, idLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
type ArgsPeticionVoto struct {
	Term         int //Cadidate term
	CadidateId   int //Cadidate requesting vote
	LastLogIndex int //idx cadidadte last log
	LastLogTerm  int //term Candidato last term
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
//
type RespuestaPeticionVoto struct {
	Term        int  //currentTerm for candiodate to update
	VoteGranded bool //True=received vote
}

// Metodo para RPC PedirVoto
//
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	if peticion.Term >= nr.currentTerm && (nr.votedFor == -1 || nr.votedFor == peticion.CadidateId) && peticion.LastLogIndex >= nr.lastApplied {
		nr.votedFor = peticion.CadidateId
		nr.currentTerm = peticion.Term
		reply.Term = peticion.Term
		reply.VoteGranded = true
		nr.estado = Seguidor
		nr.canalLat <- true

	} else {
		reply.Term = nr.currentTerm
		reply.VoteGranded = false
	}

	return nil
}

type ArgAppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []AplicaOperacion
	LeaderCommit int
}

type Results struct {
	SeguidorId int
	Term       int
	Success    bool
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	if args.Term > nr.currentTerm {
		nr.currentTerm = args.Term
	}
	if nr.estado == Seguidor {
		nr.canalLat <- true
	}
	if (nr.currentTerm >= args.Term && nr.estado == Candidato) || nr.currentTerm > args.Term {
		results.Success = false

	} else {
		results.Success = true
	}
	return nil
}

func (nr *NodoRaft) generarLatido() {
	newNextIdx := len(nr.log)
	newMatchIdx := len(nr.log) - 1
	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			nr.Mux.Lock()
			req := ArgAppendEntries{
				Term:         nr.currentTerm,
				LeaderId:     nr.Yo,
				PrevLogIndex: nr.nextIndex[i],
				Entries:      nr.log[nr.nextIndex[i]:],
				LeaderCommit: nr.comitIndex,
			}
			nr.Mux.Unlock()
			go nr.callAppendEntry(i, &req)
		}

	}
	for i := len(nr.Nodos); i >= 0; i-- {
		select {
		case reply := <-nr.appendChan:
			nr.Mux.Lock()
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
			nr.Mux.Unlock()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (nr *NodoRaft) callAppendEntry(nodo int, request *ArgAppendEntries) {
	var respuesta Results

	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntry", &request, &respuesta, 25*time.Millisecond)
	if err == nil {
		nr.appendChan <- *&respuesta

	}

}

// ----- Metodos/Funciones a utilizar como clientes
//
//

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
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
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	ok := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", &args, &reply, 150*time.Millisecond)
	if ok == nil {
		nr.replyVChan <- *reply
	}

	return true
}

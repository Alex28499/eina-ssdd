package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"

	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	//hosts
	MAQUINA1 = "127.0.0.1"
	MAQUINA2 = "127.0.0.1"
	MAQUINA3 = "127.0.0.1"

	//puertos
	PUERTOREPLICA1 = "30010"
	PUERTOREPLICA2 = "30011"
	PUERTOREPLICA3 = "30012"
	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas
	REPLICACMD = "cd /home/alejandro/Uni/3º/SistemasDist/practicas/eina-ssdd/Practica5/CodigoEsqueleto/raft  && /usr/local/go/bin/go run " + EXECREPLICA

	PRIVKEYFILE = "id_rsa"
)

// PATH de los ejecutables de modulo golang de servicio Raft
//var PATH string = filepath.Join(os.Getenv("HOME"), "tmp", "p5", "raft")

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = REPLICACMD

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	//t.Skip("SKIPPED TestPrimerasPruebas")
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{false, false, false})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })
}

// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	t.Skip("SKIPPED TestAcuerdosConFallos")
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{false, false, false})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T5:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T5:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)
	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ?? - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	//t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0
	cfg.comprobarEstadoRemoto(0, 1, false, -1)

	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto(1, 1, false, -1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto(2, 1, false, -1)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	//t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	//t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)

	cfg.desconectarLiderActual()
	time.Sleep(3 * time.Second)

	fmt.Printf("Comprobar nuevo lider\n")
	time.Sleep(3 * time.Second)
	cfg.pruebaUnLider(3)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	//t.Skip("SKIPPED tresOperacionesComprometidasEstable")

	fmt.Println(t.Name(), ".....................")
	cfg.t = t // Actualizar la estructura de datos de tests para errores

	cfg.startDistributedProcesses()
	time.Sleep(6 * time.Second)

	cmds := []raft.TipoOperacion{
		{Operacion: "leer", Clave: "1"},
		{Operacion: "leer", Clave: "2"},
		{Operacion: "escribir", Clave: "3", Valor: "mensaje"}}
	cfg.someterOps(cmds)
	time.Sleep(100 * time.Millisecond)
	// Se comprueba que el commitIndex es 2 ya que se han comprometido 3 operaciones(0,1,2)
	cfg.comprobarOp(2)
	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	fmt.Println(t.Name(), ".....................")
	cfg.t = t // Actualizar la estructura de datos de tests para errores

	cfg.startDistributedProcesses()
	time.Sleep(6 * time.Second)

	cmd := raft.TipoOperacion{Operacion: "leer", Clave: "1"}
	cfg.someterOp(cmd)
	time.Sleep(100 * time.Millisecond)
	// Se comprueba que el commitIndex es 2 ya que se han comprometido 3 operaciones(0,1,2)
	cfg.comprobarOp(0)
	cfg.desconectarSeguidor()
	cmds := []raft.TipoOperacion{
		{Operacion: "leer", Clave: "2"},
		{Operacion: "leer", Clave: "3"}}
	cfg.someterOps(cmds)
	time.Sleep(100 * time.Millisecond)
	cfg.comprobarOp(3)

	cfg.startDistributedProcesses()
	time.Sleep(6 * time.Second)

	cfg.someterOps(cmds)
	time.Sleep(100 * time.Millisecond)
	cfg.comprobarOp(5)
	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	t.Skip("SKIPPED SinAcuerdoPorFallos")
	fmt.Println(t.Name(), ".....................")
	cfg.t = t // Actualizar la estructura de datos de tests para errores

	cfg.startDistributedProcesses()

	time.Sleep(6 * time.Second)

	cmd := raft.TipoOperacion{Operacion: "leer", Clave: "1"}
	cfg.someterOp(cmd)
	time.Sleep(100 * time.Millisecond)
	// Se comprueba que el commitIndex es 2 ya que se han comprometido 3 operaciones(0,1,2)
	cfg.comprobarOp(0)
	cfg.desconectarSeguidor()
	cfg.desconectarSeguidor()
	cmds := []raft.TipoOperacion{
		{Operacion: "leer", Clave: "2"},
		{Operacion: "leer", Clave: "3"}}
	cfg.someterOps(cmds)
	time.Sleep(100 * time.Millisecond)
	cfg.comprobarOp(3)

	cfg.startDistributedProcesses()
	time.Sleep(6 * time.Second)

	cfg.someterOps(cmds)
	time.Sleep(100 * time.Millisecond)
	cfg.comprobarOp(5)
	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	//t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	fmt.Println(t.Name(), ".....................")
	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Obtener un lider y, a continuación someter una operacion
	cfg.startDistributedProcesses()
	time.Sleep(6 * time.Second)
	cmds := []raft.TipoOperacion{
		{Operacion: "escribir", Clave: "1", Valor: "mensaje1"},
		{Operacion: "leer", Clave: "2"},
		{Operacion: "leer", Clave: "3"},
		{Operacion: "escribir", Clave: "4"}}

	// Someter 5  operaciones concurrentes, un bucle para estabilizar la ejecucion
	for _, cmd := range cmds {
		go cfg.someterOps([]raft.TipoOperacion{cmd})
	}
	time.Sleep(1 * time.Second)
	cfg.comprobarOp(3)
	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(cfg.nodosRaft[i]); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	nodo rpctimeout.HostPort) (int, int, bool, int) {
	reply := &raft.EstadoRemoto{}
	fmt.Println(string(nodo))
	err := nodo.CallTimeout("NodoRaft.ObtenerEstadoNodo",
		//nodo.CallTimeout("NodoRaft.ObtenerEstadoNodo",
		&raft.Vacio{}, reply, 50*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)
	auxIdx := 0
	for _, endPoint := range cfg.nodosRaft {
		if !cfg.conectados[auxIdx] {
			despliegue.ExecMutipleNodes(EXECREPLICACMD+
				" "+strconv.Itoa(auxIdx)+" "+
				rpctimeout.HostPortArrayToString(cfg.nodosRaft),
				[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)
			cfg.conectados[auxIdx] = true
		}
		auxIdx++
		// dar tiempo para se establezcan las replicas
		//time.Sleep(1000 * time.Millisecond)
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(2000 * time.Millisecond)
}

//
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
			cfg.conectados[i] = false
		}
	}
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	cfg.t.Log("Comprobando: ", idNodoDeseado, "\n")
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(cfg.nodosRaft[idNodoDeseado])

	cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		cfg.t.Log("Estado incorrecto en replica ", idNodoDeseado, " en subtest",
			cfg.t.Name())
	}

}

func (cfg *configDespliegue) desconectarSeguidor() {
	idSeguidor := -1
	for i, _ := range cfg.nodosRaft {
		if cfg.conectados[i] {
			_, _, esLider, _ := cfg.obtenerEstadoRemoto(cfg.nodosRaft[i])
			if !esLider {
				idSeguidor = i
				err := cfg.nodosRaft[idSeguidor].CallTimeout("NodoRaft.ParaNodo",
					&raft.Vacio{}, &raft.Vacio{}, 50*time.Millisecond)
				cfg.t.Log("ParaNodo: ", err)
				check.CheckError(err, "Error en llamada RPC ParaNodo")
				cfg.conectados[idSeguidor] = false
				break
			}
		}
	}
}

func (cfg *configDespliegue) desconectarLiderActual() {
	idLider := cfg.encontrarLider()

	err := cfg.nodosRaft[idLider].CallTimeout("NodoRaft.ParaNodo",
		&raft.Vacio{}, &raft.Vacio{}, 50*time.Millisecond)
	cfg.t.Log("ParaNodo: ", err)
	check.CheckError(err, "Error en llamada RPC ParaNodo")
	cfg.conectados[idLider] = false
}

func (cfg *configDespliegue) encontrarLider() int {
	idLeader := -1
	for i, _ := range cfg.nodosRaft {
		_, _, esLider, _ := cfg.obtenerEstadoRemoto(cfg.nodosRaft[i])
		if esLider {
			idLeader = i
			break
		}
	}
	return idLeader
}

func (cfg *configDespliegue) someterOps(cmds []raft.TipoOperacion) {
	for _, cmd := range cmds {
		cfg.someterOp(cmd)
	}
}

func (cfg *configDespliegue) someterOp(cmd raft.TipoOperacion) {
	lider := cfg.encontrarLider()
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[lider].CallTimeout("NodoRaft.SometerOperacionRaft",
		cmd, &reply, 50*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC SometerOperacion")
}

func (cfg *configDespliegue) comprobarOp(n int) bool {
	var reply bool
	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			reply = false
			err := endPoint.CallTimeout("NodoRaft.CheckCommits",
				n, &reply, 50*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC CheckCommits nodo")
			if !reply {
				fmt.Println("El nodo ", i, " no tiene todas las entradas comprometidas")
			} else {
				fmt.Println("BIEN:", i)
			}
		}
	}
	return reply
}

package testintegracionraft1

import (
	"fmt"
	"net/rpc"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
	"strconv"
	"testing"
	"time"
)

const (
	//hosts
	MAQUINA_LOCAL = "127.0.0.1"
	MAQUINA1      = "127.0.0.1"
	MAQUINA2      = "127.0.0.1"
	MAQUINA3      = "127.0.0.1"

	//puertos
	PUERTOREPLICA1 = "30000"
	PUERTOREPLICA2 = "30001"
	PUERTOREPLICA3 = "30002"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// PATH de los ejecutables de modulo golang de servicio de vistas
	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001
	//Uni/3º/SistemasDist/practicas/eina-ssdd/practica1/MasterWorker/Worker && /usr/local/go/bin/go run worker.go
	// go run testcltvts/main.go 127.0.0.1:29003 127.0.0.1:29001 127.0.0.1:29000
	REPLICACMD = "cd Uni/3º/SistemasDist/practicas/eina-ssdd/Practica4/CodigoEsqueleto/raft && /usr/local/go/bin/go run " + EXECREPLICA

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_rsa"
)

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cr := make(CanalResultados, 2000)

	// Run test sequence

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparadaTest1",
		func(t *testing.T) { cr.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLiderTest2",
		func(t *testing.T) { cr.ElegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLiderTest3",
		func(t *testing.T) { cr.FalloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Primer nodo copia

	// tear down code
	// eliminar procesos en máquinas remotas
	cr.stop()
}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type CanalResultados chan string

func (cr CanalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cr *CanalResultados) startDistributedProcesses(
	replicasMaquinas map[string]string) {
	fmt.Println("Starting ")
	listaReplicas := ""
	for replica := range replicasMaquinas {
		listaReplicas = listaReplicas + " " + replica
	}
	auxIdx := 0
	for _, maquina := range replicasMaquinas {
		despliegue.ExecMutipleNodes(
			REPLICACMD+" "+strconv.Itoa(auxIdx)+" "+listaReplicas,
			[]string{maquina}, *cr, PRIVKEYFILE)
		auxIdx++

		// dar tiempo para se establezcan las replicas
	}
	time.Sleep(5 * time.Second)

}

//
func (cr *CanalResultados) stopDistributedProcesses(replicas []string) {
	fmt.Println("Stopping")
	for _, replica := range replicas {
		fmt.Println("Replica a parar: " + replica)
		cliente, err := rpc.Dial("tcp", replica)
		check.CheckError(err, "error tcp")
		if cliente != nil {

			err = rpctimeout.CallTimeout(cliente, "NodoRaft.Para", struct{}{}, &struct{}{}, 25*time.Millisecond)
			check.CheckError(err, "Error al parar")
		}
	}

}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ??
func (cr *CanalResultados) soloArranqueYparadaTest1(t *testing.T) {
	//t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	// Poner en marcha replicas en remoto
	cr.startDistributedProcesses(map[string]string{REPLICA1: MAQUINA1})

	fmt.Println("Iniciado")
	// Parar réplicas alamcenamiento en remoto

	cr.stopDistributedProcesses([]string{REPLICA1})

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha
func (cr *CanalResultados) ElegirPrimerLiderTest2(t *testing.T) {
	//t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	// Poner en marcha  3 réplicas Raft
	replicasMaquinas :=
		map[string]string{REPLICA1: MAQUINA1, REPLICA2: MAQUINA2, REPLICA3: MAQUINA3}
	cr.startDistributedProcesses(replicasMaquinas)

	fmt.Printf("Probando lider en curso\n")
	time.Sleep(2 * time.Second)
	if cr.pruebaUnLider([]string{REPLICA1, REPLICA2, REPLICA3}) == "-1" {
		t.Errorf("No existe lider")
	}

	// Parar réplicas alamcenamiento en remoto
	cr.stopDistributedProcesses([]string{REPLICA1, REPLICA2, REPLICA3})

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo
func (cr *CanalResultados) FalloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	//t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	// Poner en marcha  3 réplicas Raft
	replicasMaquinas :=
		map[string]string{REPLICA1: MAQUINA1, REPLICA2: MAQUINA2, REPLICA3: MAQUINA3}
	cr.startDistributedProcesses(replicasMaquinas)

	lider := cr.pruebaUnLider([]string{REPLICA1, REPLICA2, REPLICA3})

	if lider != "-1" {
		cr.stopDistributedProcesses([]string{lider})
	}
	time.Sleep(2 * time.Second)
	newArray := cr.eliminarLider(lider, []string{REPLICA1, REPLICA2, REPLICA3})
	fmt.Println("Comprobar nuevo lider", newArray)

	lider1 := cr.pruebaUnLider(newArray)

	if lider1 != "-1" {
		t.Errorf("No existe lider")
	}
	// Parar réplicas almacenamiento en remoto
	cr.stopDistributedProcesses(newArray)

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos
func (cr *CanalResultados) tresOperacionesComprometidasEstable(t *testing.T) {
	//t.Skip("SKIPPED tresOperacionesComprometidasEstable")

	fmt.Println(t.Name(), ".....................")

	replicasMaquinas :=
		map[string]string{REPLICA1: MAQUINA1, REPLICA2: MAQUINA2, REPLICA3: MAQUINA3}
	cr.startDistributedProcesses(replicasMaquinas)
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cr *CanalResultados) pruebaUnLider(replicas []string) string {
	fmt.Println("Probar lider")
	existeLider := "-1"
	for _, replica := range replicas {
		cliente, err := rpc.Dial("tcp", replica)
		check.CheckError(err, "error tcp")
		if cliente != nil {
			reply := new(raft.ReplyEstado)
			err = rpctimeout.CallTimeout(cliente, "NodoRaft.ObtenerEstado", struct{}{}, &reply, 25*time.Millisecond)
			fmt.Println("Reply ", reply.Yo, reply)
			if reply.Lider {
				existeLider = replicas[reply.Yo]
			}
			check.CheckError(err, "Error al parar")
		}
	}
	return existeLider
}

func (cr *CanalResultados) eliminarLider(lider string, replica []string) []string {
	for i, v := range replica {
		if v == lider {
			return append(replica[:i], replica[i+1:]...)
		}
	}
	return replica
}

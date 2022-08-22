/*
* AUTOR: Rafael Tolosana Calasanz
* Modificado
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: server.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes a la práctica 3
 */
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"practica3/com"
	"practica3/sshcom"
	"strings"
	"syscall"
	"time"
)

type PrimesImpl struct{}

type peticion struct {
	intervalo  com.TPInterval
	cliente    chan []int
	clienteErr chan error
}

var peticiones = make(chan peticion, 5)

func remoteExecution(IPHost string, comando string) {
	s := strings.Split(IPHost, ":")
	ssh, err := sshcom.NewSshClient(
		"alejandro",
		s[0],
		22,
		"/home/alejandro/.ssh/id_rsa",
		"")

	if err != nil {
		fmt.Printf("SSH init error %v", err)
	} else {
		output, err := ssh.RunCommand(comando)
		fmt.Println(output)
		if err != nil {
			fmt.Printf("SSH run command error %v", err)
		}
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func lineCounter(fichero string) int {
	file, _ := os.Open(fichero)
	fileScanner := bufio.NewScanner(file)
	lineCount := 0
	for fileScanner.Scan() {
		lineCount++
	}
	fmt.Println("number of lines:", lineCount)
	file.Close()
	return lineCount
}

func main() {

	if len(os.Args) < 2 || len(os.Args) >= 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s host:port", os.Args[0])
		os.Exit(1)
	}
	maxconexion := lineCounter("worker.txt")
	peticiones = make(chan peticion, maxconexion)
	/*msg := ms.New(id, "bully.txt")
	bully := b.NewBully(id, mNodes, msg)
	go bully.Init()
	bully.EsperarLider()*/
	initworkers()
	go proxyClientDeclartion()
	for {
	}
}

func handleRequest(dir string) {
	fmt.Println("")
	worker, err := rpc.DialHTTP("tcp", dir)
	if err != nil {
		fmt.Printf("Error al conectar con worker %v:%v\n", dir, err)
	}
	for {
		var listaPrimos []int
		peticion := <-peticiones
		fmt.Println("Recibe peticion")
		//listaPrimos, crash := interaccion(worker, peticion, 425, 3)
		listaPrimos, crash := interaccion(worker, peticion, 750, 3)
		//listaPrimos, crash := interaccion(worker, peticion, 1000, 3)
		peticion.cliente <- listaPrimos
		//peticion.clienteErr <- err
		peticion.clienteErr <- nil

		if crash {
			fmt.Print("TERMINA CONEXION-WORKER\n")
			break
		}
	}
}

func interaccion(worker *rpc.Client, pet peticion, timeout int, numIntentos int) ([]int, bool) {
	var listaPrimos []int
	t := time.Duration(timeout) * time.Millisecond
	for count := 0; count < numIntentos; count++ {
		resultCall := worker.Go("PrimesImpl.FindPrimes", pet.intervalo, &listaPrimos, nil)
		select {
		case <-resultCall.Done:
			if resultCall.Error == nil {
				fmt.Println("RESPUESTA")
				return listaPrimos, false
			} else {
				//err := errors.New("CRASH")
				fmt.Println("CRASH")
				return nil, true
			}
		case <-time.After(t):
			if count < numIntentos {
				//t = t * 2
				t = time.Duration(timeout) * time.Duration(numIntentos)
				//t = time.Duration(timeout)
				fmt.Print("TIMEOUT: ")
				fmt.Println(t)
				fmt.Println("AUMENTAMOS TIMEOUT")
			}
		}
	}
	fmt.Println("SALIMOS POR OMISSION-DELAY")
	//err := errors.New("OMISSION-DELAY")
	return nil, false
}

func setWorker(IPHost string) {
	comando := "cd Uni/3º/SistemasDist/practicas/eina-ssdd/practica3/ && /usr/local/go/bin/go run worker.go " + IPHost
	go remoteExecution(IPHost, comando)
	fmt.Printf("Lanzado: %s\n ", IPHost)
	time.Sleep(20 * time.Second)
	fmt.Printf("me despierto")
	go handleRequest(IPHost)
}

func initworkers() {
	f, err := os.Open("worker.txt")
	if err != nil {
		checkError(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		go setWorker(scanner.Text())
	}
}

func catch(maxCon int, addr []string) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig

	os.Exit(0)
}

func proxyClientDeclartion() {
	//fmt.Println("Conexion cliente")
	primesImpl := new(PrimesImpl)
	rpc.Register(primesImpl)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", os.Args[1])
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Printf("escuchando en %v\n", os.Args[1])

	http.Serve(l, nil)
}

func (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int) error {
	canal := make(chan []int)
	canalError := make(chan error)

	//fmt.Println("peticion ha encolar")

	peticiones <- peticion{
		intervalo:  interval,
		cliente:    canal,
		clienteErr: canalError,
	}

	//fmt.Println("peticion encolada")

	*primeList = <-canal

	//fmt.Println("Peticion resuelta")

	return <-canalError
}

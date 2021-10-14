/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: server.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes a la práctica 1
 */
package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"practica1/com"
	"practica1/sshcom"
	"strings"
	"time"
)

const (
	tts        = 10000
	maxrequest = 6
)

func remoteExecution(IPHost string, comando string) {
	s := strings.Split(IPHost, ":")
	ssh, err := sshcom.NewSshClient(
		"a760628",
		//"alejandro",
		s[0],
		22,
		"/home/a760628/.ssh/id_rsa",
		//"/home/alejandro/.ssh/id_rsa",
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

var (
	finrequest     = make(chan string, maxrequest)
	requestbalacer = make(chan com.Request, 1)
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func main() {

	if len(os.Args) < 2 || len(os.Args) >= 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s host:port", os.Args[0])
		os.Exit(1)
	}

	CONN_TYPE := "tcp"
	CONN_HOST_PORT := os.Args[1]

	listener, err := net.Listen(CONN_TYPE, CONN_HOST_PORT)
	checkError(err)

	conn, err := listener.Accept()
	defer conn.Close()
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	var buffer com.Request
	initworkers(*enc)
	checkError(err)
	for {
		<-finrequest
		dec.Decode(&buffer)
		requestbalacer <- buffer
	}
}

func handleRequest(ip string, cli gob.Encoder) {
	finrequest <- "ok"
	tcpAddr, err := net.ResolveTCPAddr("tcp", ip)
	checkError(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	var reply com.Reply
	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	for {

		peticion := <-requestbalacer
		encoder.Encode(peticion)
		err = decoder.Decode(&reply)
		checkError(err)
		cli.Encode(reply)
		finrequest <- "ok"
	}

}

func setWorker(IPHost string, enc gob.Encoder) {
	comando := "cd practica1/MasterWorker/Worker && /usr/local/go/bin/go run worker.go " + IPHost
	//comando := "cd Uni/3º/SistemasDist/practicas/eina-ssdd/practica1/MasterWorker/Worker && /usr/local/go/bin/go run worker.go " + IPHost
	go remoteExecution(IPHost, comando)
	fmt.Printf("Lanzado: %s\n ", IPHost)
	time.Sleep(time.Duration(tts) * time.Millisecond)
	fmt.Printf("me despierto")
	go handleRequest(IPHost, enc)
}

func initworkers(enc gob.Encoder) {
	f, err := os.Open("worker.txt")
	if err != nil {
		checkError(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		go setWorker(scanner.Text(), enc)
	}
}

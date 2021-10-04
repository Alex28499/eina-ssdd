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
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"os/exec"
	"practica1/com"
)

const (
	maxrequest = 3
)

var (
	finrequest = make(chan string, maxrequest)
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
	initChannel()
	checkError(err)
	for {
		<-finrequest
		dec.Decode(&buffer)
		go handleRequest(*enc, buffer)
	}
}

func handleRequest(enc gob.Encoder, buffer com.Request) {
	cmd := exec.Command("ssh", "127.0.0.0.1:25000", "mkdir hola")
}

func initChannel() {
	for i := 0; i < maxrequest; i++ {
		finrequest <- "ok"
	}
}

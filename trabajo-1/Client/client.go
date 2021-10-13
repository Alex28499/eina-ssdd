/*
* AUTOR: Rafael Tolosana Calasanz
* MODIFICADO: Alejandro Terron(761069) y Oscar Anadon(760628)
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: octubre de 2021
* FICHERO: client.go
* DESCRIPCIÓN: cliente completo para el escenario de entrada de parametros por terminal y serializado/deserializado
 */
package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func sendIntervalo(conn net.Conn, desde int, hasta int) {
	bytesDesde := make([]byte, 4)
	bytesHasta := make([]byte, 4)

	binary.LittleEndian.PutUint32(bytesDesde, uint32(desde))
	binary.LittleEndian.PutUint32(bytesHasta, uint32(hasta))
	conn.Write([]byte(append(bytesDesde, bytesHasta...)))

}

func recvSolucion(conn net.Conn) {
	numMax := make([]byte, 4)
	_, err := conn.Read(numMax)
	checkError(err)
	auxMax := int(binary.LittleEndian.Uint32(numMax))
	bufferBytes := make([]byte, auxMax*4)
	_, err = conn.Read(bufferBytes)
	fmt.Printf("Me llegan %v    %v\n", auxMax, bufferBytes)
	checkError(err)
	maxPrimos := len(bufferBytes) / 4
	matrixPrimos := make([]int, maxPrimos)
	for i := 0; i < maxPrimos; i++ {
		matrixPrimos[i] = int(binary.LittleEndian.Uint32(bufferBytes[i*4:]))
	}
	fmt.Println(matrixPrimos)
}

func main() {
	endpoint := "155.210.154.200:30000"

	// TODO: crear el intervalo solicitando dos números por teclado interval := com.TPInterval{1000, 70000}
	var desde int
	var hasta int
	fmt.Println("Introduzca el intervalo")
	fmt.Printf("Desde=")
	fmt.Scanf("%d\n", &desde)
	fmt.Printf("Hasta=")
	fmt.Scanf("%d\n", &hasta)

	tcpAddr, err := net.ResolveTCPAddr("tcp", endpoint)
	checkError(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	defer conn.Close()

	sendIntervalo(conn, desde, hasta)
	recvSolucion(conn)
	// la variable conn es de tipo *net.TCPconn
}

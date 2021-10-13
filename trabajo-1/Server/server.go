/*
* AUTOR: Rafael Tolosana Calasanz
* MODIFICADO: Alejandro Terron(761069) y Oscar Anadon(760628)
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: octubre de 2021
* FICHERO: server.go
* DESCRIPCIÓN: servidor completo para el escenario de serializado/deserializado
 */
package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"trabajo-1/com"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// PRE: verdad
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func IsPrime(n int) (foundDivisor bool) {
	foundDivisor = false
	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]
func FindPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.A; i <= interval.B; i++ {
		if IsPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

func respClient(conn net.Conn, listaPrimos []int) {
	numPrimos := len(listaPrimos)
	bytesResp := make([]byte, numPrimos*4)
	bytesPrimo := make([]byte, 4)
	for i := 0; i < numPrimos; i++ {
		binary.LittleEndian.PutUint32(bytesPrimo, uint32(listaPrimos[i]))
		for j := 0; j < 4; j++ {
			bytesResp[i*4+j] = bytesPrimo[j]
		}
	}
	binary.LittleEndian.PutUint32(bytesPrimo, uint32(numPrimos))
	conn.Write([]byte(bytesPrimo))
	conn.Write([]byte(bytesResp))
}

func main() {

	endpoint := "155.210.154.200:30000"

	listener, err := net.Listen("tcp", endpoint)
	checkError(err)

	conn, err := listener.Accept()
	defer conn.Close()
	checkError(err)
	bufferIntervalo := make([]byte, 2*4)
	_, err = conn.Read(bufferIntervalo)
	desde := int(binary.LittleEndian.Uint32(bufferIntervalo[0:]))
	hasta := int(binary.LittleEndian.Uint32(bufferIntervalo[4:]))
	intervalo := com.TPInterval{A: desde, B: hasta}
	listaPrimos := FindPrimes(intervalo)
	respClient(conn, listaPrimos)
}

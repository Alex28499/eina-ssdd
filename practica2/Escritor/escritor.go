// AUTORES: Alejandro Terrón Álvarez / Sergio Torres Castillo
// NIAs: 761069 / 738233
// FICHERO: escritor.go
// FECHA: 24/10/2020
// TIEMPO: <30 MINUTOS>
// DESCRIPCIÓN: <Codigo de apoyo para testear el funcionamiento relativo a las operaciones de escritura>

package main

import (
	"os"
	"time"

	"strconv"

	db "practica2/SharedDB"
	ag "practica2/ra"
)

func main() {
	id, _ := strconv.Atoi(os.Args[1])
	mNodes, _ := strconv.Atoi(os.Args[2])
	time.Sleep(time.Second)
	file := db.New("databaseofwriter" + os.Args[1])
	agr := ag.New(id, mNodes, "WRITE", "users.txt", file)

	for count := 0; count < 5; count++ {
		agr.PreProtocol()
		file.EscribirFichero("escritura de " + strconv.Itoa(id) + "\n")
		agr.CopyDB()
		agr.PostProtocol()
	}
	time.Sleep(5 * time.Second)
}

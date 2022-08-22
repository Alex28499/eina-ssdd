package shareddb

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

type FileDB struct {
	file string
}

func isError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}
	return (err != nil)
}

func New(name string) *FileDB {
	//	fmt.Printf("----------->%v\n", name)
	var _, err = os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if isError(err) {
		os.Exit(1)
	}
	return &FileDB{file: name}
}

func (fl *FileDB) LeerFichero() {
	//fmt.Println("------------------------------------>leyendo")
	var file, err = os.OpenFile(fl.file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	defer file.Close()
	if isError(err) {
		os.Exit(1)
	}
	time.Sleep(1 * time.Second)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(">", scanner.Text())
	}
	//fmt.Println("<------------------------------------leido")
}

func (fl *FileDB) EscribirFichero(frase string) {
	//fmt.Println("------------------------------------>Escribiendo")
	//fmt.Println(frase)
	var file, err = os.OpenFile(fl.file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	defer file.Close()
	if isError(err) {
		os.Exit(1)
	}
	_, err = file.WriteString(frase)
	if isError(err) {
		os.Exit(1)
	}
	time.Sleep(1 * time.Second)
	//fmt.Println("<------------------------------------Escrito")
}

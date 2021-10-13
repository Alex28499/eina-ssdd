/*
* AUTOR: stefanprodan (https://gist.github.com/stefanprodan/2d20d0c6fdab6f14ce8219464e8b4b9a)
* MODIFICADO: Alejandro Terron(761069) y Oscar Anadon(760628)
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: octubre de 2021
* FICHERO: lanzar.go
* DESCRIPCIÓN: adaptacion del codigo de lanzamiento de comandos a traves de ssh en go, disponible
* en el repositorio en github ofrecido en la memoria. Ha sido adaptado al lanzamiento en hendrix.
 */

package main

import (
	"fmt"
	"log"
	"os"
	"trabajo-1/sshcom"
)

func remoteExecution(username string, IPHost string, rsa string, comando string) {
	ssh, err := sshcom.NewSshClient(
		username,
		IPHost,
		22,
		rsa,
		"")

	if err != nil {
		log.Printf("SSH init error %v", err)
	} else {
		output, err := ssh.RunCommand(comando)
		fmt.Println(output)
		if err != nil {
			log.Printf("SSH run command error %v", err)
		}
	}
}

func main() {
	var username, ipHost, rsa, fichero, comando string

	fmt.Println("Introduzca los siguientes datos")
	fmt.Printf("NombreUsuario=")
	fmt.Scanf("%s\n", &username)
	fmt.Printf("IPHost=")
	fmt.Scanf("%s\n", &ipHost)
	fmt.Printf("Ejecutar C (Cliente) o ejecutar S (Servidor)=")
	fmt.Scanf("%s\n", &fichero)
	if fichero == "C" {
		comando = "cd trabajo-1/Client && /usr/local/go/bin/go run client.go"
	} else if fichero == "S" {
		comando = "cd trabajo-1/Server && /usr/local/go/bin/go run server.go"
	} else {
		fmt.Printf("Introduce C o S")
		os.Exit(1)
	}
	rsa = "/home/" + username + "/.ssh/id_rsa"

	remoteExecution(username, ipHost, rsa, comando)
}

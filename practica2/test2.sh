#!/usr/bin/env bash


go run escritor.go 1 4 4 &
go run escritor.go 2 4 4 &
go run lector.go 3 4 4 &
go run lector.go 4 4 4 &

wait $!

./shiviz.sh test2.log &> /dev/null &

exit
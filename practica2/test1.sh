#!/usr/bin/env bash

go run lector.go 1 4 4 &
go run lector.go 2 4 4 &
go run lector.go 3 4 4 &
go run lector.go 4 4 4 &

wait $!

./shiviz.sh test1.log &> /dev/null &

exit
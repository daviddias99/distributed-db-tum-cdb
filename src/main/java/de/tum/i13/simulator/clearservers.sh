#!/bin/bash

declare -i myvar=25565+$1

for VARIABLE in $( eval echo {25565..$myvar} )
do
  echo $VARIABLE
	kill -9 $(sudo lsof -t -i:$VARIABLE)
done
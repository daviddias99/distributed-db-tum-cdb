#!/bin/bash

declare -i myvar=35660+$1

for VARIABLE in $( eval echo {35660..$myvar} )
do
  echo $VARIABLE
	kill -9 $(sudo lsof -t -i:$VARIABLE)
done

for VARIABLE in 25670
do
  echo $VARIABLE
	kill -9 $(sudo lsof -t -i:$VARIABLE)
done
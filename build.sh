#!/bin/bash

mvn clean package $1
if [ -d bin ]; then
	echo "bin directory exist"
else
	mkdir bin
fi
cp target/TCP_Port_Mapper-*-jar-with-dependencies.jar bin/TCP_Port_Mapper.jar
cp *.properties bin/


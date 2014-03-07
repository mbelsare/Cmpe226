#!/bin/bash
javac -d bin -sourcepath src -cp "lib/*" src/*.java
for dir in /Users/raul/Desktop/split1/*
do
    echo 'Rahul'
    for file in "$dir"/*.txt
    do
	java -cp bin:lib/* ReadXMLUTF8FileSAX "$file"
	echo $file
        javapid=$!
	echo "$javapid"	
	kill "$javapid"
    done
done
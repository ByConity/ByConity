#!/usr/bin/env bash
ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

mvn clean install
cp input-formats/target/input-formats-1.0-SNAPSHOT.jar cpp

if [  -z "$JAVA_HOME" ]
then
    echo "JAVA_HOME not set"
    exit 1
else
    echo "JAVA_HOME: $JAVA_HOME"
fi

cd cpp
cmake -Bbuild -DCMAKE_BUILD_TYPE=Release && cmake --build build -v


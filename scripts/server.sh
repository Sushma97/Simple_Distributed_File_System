#!/bin/bash
mp1_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$mp1_dir"

java -cp ../target/mp1_cs425_grep-1.0-SNAPSHOT-jar-with-dependencies.jar com.cs425.Server $1
#!/bin/sh
mvn clean install
java -jar target/microbenchmarks.jar -tu s -f 2 -wi 2 -i 5  "com.hazelcast.localmapstatsperformance.GetLocalStatsBenchmark.*"


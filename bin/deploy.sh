#!/usr/bin/env bash
mvn install
hdfs dfs -rm -r output
hadoop jar target/inverted-index-1.0-SNAPSHOT.jar com.zsuun.InvertedIndex data output
rm -rf output
hdfs dfs -get output
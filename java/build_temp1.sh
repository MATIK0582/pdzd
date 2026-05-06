#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${HADOOP_HOME:-}" ]]; then
  echo "ERROR: HADOOP_HOME is not set. Example: export HADOOP_HOME=/usr/local/hadoop-3.3.5" >&2
  exit 1
fi

export HADOOP_CLASSPATH="$(hadoop classpath)"

rm -rf temp1_classes Temp1MapReduce.jar
mkdir -p temp1_classes

javac -source 1.8 -target 1.8 -classpath "$HADOOP_CLASSPATH" -d temp1_classes Temp1MapReduce.java
jar -cvf Temp1MapReduce.jar -C temp1_classes/ .

echo "Built: Temp1MapReduce.jar"
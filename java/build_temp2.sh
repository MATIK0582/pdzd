#!/usr/bin/env bash
set -euo pipefail

if ! command -v hadoop >/dev/null 2>&1; then
  echo "ERROR: hadoop command not found. Check HADOOP_HOME and PATH." >&2
  exit 1
fi

if ! command -v javac >/dev/null 2>&1; then
  echo "ERROR: javac command not found. Check JAVA_HOME and PATH." >&2
  exit 1
fi

if ! command -v jar >/dev/null 2>&1; then
  echo "ERROR: jar command not found. Check JAVA_HOME and PATH." >&2
  exit 1
fi

export HADOOP_CLASSPATH="$(hadoop classpath)"

rm -rf temp2_classes Temp2MapReduce.jar
mkdir -p temp2_classes

javac -source 1.8 -target 1.8 \
  -classpath "$HADOOP_CLASSPATH" \
  -d temp2_classes \
  Temp2MapReduce.java

jar -cvf Temp2MapReduce.jar -C temp2_classes/ .
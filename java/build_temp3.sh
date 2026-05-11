#!/usr/bin/env bash
set -euo pipefail

if ! command -v hadoop >/dev/null 2>&1; then
  echo "ERROR: hadoop command not found in PATH" >&2
  exit 1
fi

if ! command -v javac >/dev/null 2>&1; then
  echo "ERROR: javac command not found in PATH" >&2
  exit 1
fi

if ! command -v jar >/dev/null 2>&1; then
  echo "ERROR: jar command not found in PATH" >&2
  exit 1
fi

export HADOOP_CLASSPATH="$(hadoop classpath)"

rm -rf temp3_classes Temp3MapReduce.jar
mkdir -p temp3_classes

javac -source 1.8 -target 1.8 \
  -classpath "$HADOOP_CLASSPATH" \
  -d temp3_classes \
  Temp3MapReduce.java

jar -cvf Temp3MapReduce.jar -C temp3_classes/ .

echo "Built: Temp3MapReduce.jar"

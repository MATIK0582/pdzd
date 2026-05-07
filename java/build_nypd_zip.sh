#!/usr/bin/env bash
set -euo pipefail

# Kompilacja NypdZipMapReduce.java do pliku NypdZipMapReduce.jar
# Środowisko docelowe:
#   Hadoop 3.3.5
#   Java 8
#   Bez zewnętrznych bibliotek
#
# Użycie:
#   chmod +x build_nypd_zip.sh
#   ./build_nypd_zip.sh
#
# Opcjonalnie można nadpisać nazwy:
#   SRC_FILE=MojaNazwa.java JAR_FILE=MojaNazwa.jar ./build_nypd_zip.sh

SRC_FILE="${SRC_FILE:-NypdZipMapReduce.java}"
CLASS_DIR="${CLASS_DIR:-nypdzip_classes}"
JAR_FILE="${JAR_FILE:-NypdZipMapReduce.jar}"
MAIN_CLASS="${MAIN_CLASS:-NypdZipMapReduce}"

if ! command -v hadoop >/dev/null 2>&1; then
  echo "ERROR: Nie znaleziono komendy hadoop w PATH." >&2
  echo "Sprawdź, czy Hadoop jest zainstalowany i czy zmienne środowiskowe są załadowane." >&2
  exit 1
fi

if ! command -v javac >/dev/null 2>&1; then
  echo "ERROR: Nie znaleziono komendy javac w PATH." >&2
  echo "Sprawdź instalację JDK, np. java-1.8.0-openjdk-devel." >&2
  exit 1
fi

if ! command -v jar >/dev/null 2>&1; then
  echo "ERROR: Nie znaleziono komendy jar w PATH." >&2
  echo "Sprawdź instalację JDK, nie tylko JRE." >&2
  exit 1
fi

if [[ ! -f "$SRC_FILE" ]]; then
  echo "ERROR: Nie znaleziono pliku źródłowego: $SRC_FILE" >&2
  echo "Uruchom skrypt w katalogu, w którym znajduje się NypdZipMapReduce.java." >&2
  exit 1
fi

# Dla Hadoop 3.3.5 kompilacja powinna używać pełnego classpath Hadoopa.
export HADOOP_CLASSPATH="${HADOOP_CLASSPATH:-$(hadoop classpath)}"

if [[ -z "$HADOOP_CLASSPATH" ]]; then
  echo "ERROR: HADOOP_CLASSPATH jest pusty." >&2
  exit 1
fi

echo "Budowanie JAR dla $MAIN_CLASS"
echo "  Source:    $SRC_FILE"
echo "  Classes:   $CLASS_DIR"
echo "  JAR:       $JAR_FILE"
echo "  Java:      $(java -version 2>&1 | head -n 1)"
echo "  javac:     $(javac -version 2>&1)"

rm -rf "$CLASS_DIR" "$JAR_FILE"
mkdir -p "$CLASS_DIR"

javac -source 1.8 -target 1.8 \
  -encoding UTF-8 \
  -classpath "$HADOOP_CLASSPATH" \
  -d "$CLASS_DIR" \
  "$SRC_FILE"

jar -cvf "$JAR_FILE" -C "$CLASS_DIR"/ .

if [[ ! -f "$JAR_FILE" ]]; then
  echo "ERROR: Nie udało się utworzyć $JAR_FILE." >&2
  exit 1
fi

echo ""
echo "Gotowe: $JAR_FILE"
echo "Uruchomienie przykładowe:"
echo "  yarn jar $JAR_FILE $MAIN_CLASS /datasets/static/NYPD.csv /datasets/static/MODZCTA.csv /datasets/results"

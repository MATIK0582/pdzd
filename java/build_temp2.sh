export HADOOP_CLASSPATH="$(hadoop classpath)"

mkdir -p temp2_classes

javac -source 1.8 -target 1.8 \
  -classpath "$HADOOP_CLASSPATH" \
  -d temp2_classes \
  Temp2MapReduce.java

jar -cvf Temp2MapReduce.jar -C temp2_classes/ .
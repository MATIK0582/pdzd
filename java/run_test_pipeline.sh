#!/usr/bin/env bash
set -euo pipefail

# Example HDFS upload and run sequence for test datasets.
hdfs dfs -mkdir -p /datasets/test /datasets/results_test
hdfs dfs -put -f DOHMH_latest_test.csv /datasets/test/DOHMH_latest_test.csv
hdfs dfs -put -f PLUTO_test.csv /datasets/test/PLUTO_test.csv
hdfs dfs -put -f NYPD_test.csv /datasets/test/NYPD_test.csv
hdfs dfs -put -f MODZCTA_test.csv /datasets/test/MODZCTA_test.csv
hdfs dfs -setrep -w 3 /datasets/test/*.csv

# 1) Spatial enrichment NYPD -> ZIP
yarn jar NypdZipMapReduce.jar NypdZipMapReduce   /datasets/test/NYPD_test.csv   /datasets/test/MODZCTA_test.csv   /datasets/results_test

# 2) TEMP1
yarn jar Temp1MapReduce.jar Temp1MapReduce   /datasets/test/DOHMH_latest_test.csv   /datasets/test/PLUTO_test.csv   /datasets/results_test

# 3) TEMP2
yarn jar Temp2MapReduce.jar Temp2MapReduce   /datasets/results_test/temp1_latest.csv   /datasets/results_test/nypd_zip_latest.csv   /datasets/results_test

# 4) TEMP3
yarn jar Temp3MapReduce.jar Temp3MapReduce   /datasets/results_test/temp2_latest.csv   /datasets/results_test

hdfs dfs -ls /datasets/results_test
hdfs dfs -cat /datasets/results_test/temp3_latest.csv
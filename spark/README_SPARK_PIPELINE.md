# Spark pipeline: odpowiednik transformacji MapReduce

Notebook `spark_full_pipeline_mapreduce_equivalent.ipynb` zawiera cztery etapy:
1. TEMP1: DOHMH + PLUTO
2. NYPD_ZIP: przypisanie ZIP na podstawie MODZCTA
3. TEMP2: połączenie z agregatami przestępczości
4. TEMP3: finalny zbiór wynikowy

Domyślne wejścia HDFS:
- /datasets/dohmh/DOHMH_latest.csv
- /datasets/static/PLUTO.csv
- /datasets/static/NYPD.csv
- /datasets/static/MODZCTA.csv

Domyślne wyjścia HDFS:
- /datasets/results/temp1_latest.csv
- /datasets/results/nypd_zip_latest.csv
- /datasets/results/temp2_latest.csv
- /datasets/results/temp3_latest.csv

Aby uruchomić całość, w notebooku ustaw:
RUN_FULL_PIPELINE = True

Do testów można podmienić ścieżki na katalogi:
- /datasets/test
- /datasets/results_test

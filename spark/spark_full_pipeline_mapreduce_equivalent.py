
# %% [markdown]
# # Spark pipeline dla projektu DOHMH + PLUTO + NYPD
# 
# Notebook przepisuje końcowe transformacje MapReduce na PySpark uruchamiany z Jupytera w trybie `yarn`.
# 
# Etapy:
# 1. `TEMP1`: czyszczenie DOHMH, agregacje i połączenie z PLUTO.
# 2. `NYPD_ZIP`: przypisanie rekordów NYPD do ZIP/MODZCTA na podstawie współrzędnych.
# 3. `TEMP2`: połączenie `TEMP1` z agregatami przestępczości.
# 4. `TEMP3`: finalny zbiór wskaźników.
# 
# Kod zapisuje pliki wersjonowane i pliki `latest` w HDFS, analogicznie do wersji MapReduce.
# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import types as T

from datetime import datetime
import json
import re

spark = (
    SparkSession.builder
    .appName("RestaurantSafetySparkPipeline")
    .master("yarn")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Spark master:", spark.sparkContext.master)
print("Application ID:", spark.sparkContext.applicationId)
# %% [markdown]
# ## Konfiguracja ścieżek HDFS
# %%
DOHMH_INPUT = "hdfs:///datasets/dohmh/DOHMH_latest.csv"
PLUTO_INPUT = "hdfs:///datasets/static/PLUTO.csv"
NYPD_INPUT = "hdfs:///datasets/static/NYPD.csv"
MODZCTA_INPUT = "hdfs:///datasets/static/MODZCTA.csv"
RESULTS_DIR = "hdfs:///datasets/results"

# Do szybkich testów można podmienić ścieżki na:
# DOHMH_INPUT = "hdfs:///datasets/test/DOHMH_latest_test.csv"
# PLUTO_INPUT = "hdfs:///datasets/test/PLUTO_test.csv"
# NYPD_INPUT = "hdfs:///datasets/test/NYPD_test.csv"
# MODZCTA_INPUT = "hdfs:///datasets/test/MODZCTA_test.csv"
# RESULTS_DIR = "hdfs:///datasets/results_test"

CURRENT_YEAR = datetime.now().year
RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
print("Timestamp uruchomienia:", RUN_TS)
# %% [markdown]
# ## Funkcje pomocnicze
# %%
def canonical_name(name: str) -> str:
    if name is None:
        return ""
    x = name.strip().replace("\ufeff", "")
    x = re.sub(r"[^A-Za-z0-9]+", "_", x)
    x = re.sub(r"_+", "_", x).strip("_")
    return x.upper()


def read_csv(path: str):
    df = (
        spark.read
        .option("header", "true")
        .option("escape", "\"")
        .option("quote", "\"")
        .option("multiLine", "false")
        .csv(path)
    )
    for c in df.columns:
        df = df.withColumnRenamed(c, canonical_name(c))
    return df


def col_or_lit(df, *names, default=""):
    cols = set(df.columns)
    for n in names:
        cn = canonical_name(n)
        if cn in cols:
            return F.col(cn)
    return F.lit(default)


def only_digits_col(c):
    return F.regexp_replace(F.coalesce(c.cast("string"), F.lit("")), r"[^0-9]", "")


def first_five_digits_col(c):
    d = only_digits_col(c)
    return F.when(F.length(d) >= 5, F.substring(d, 1, 5)).otherwise(d)


def safe_double(c):
    txt = F.trim(F.coalesce(c.cast("string"), F.lit("")))
    return F.when(txt.rlike(r"^-?[0-9]+(\.[0-9]+)?$"), txt.cast("double"))


def safe_int(c):
    txt = F.trim(F.coalesce(c.cast("string"), F.lit("")))
    return F.when(txt.rlike(r"^[0-9]+$"), txt.cast("int"))


def norm_bbl_expr(c):
    d = only_digits_col(c)
    return (
        F.when(F.length(d) != 10, F.lit(""))
         .when(d == F.lit("0000000000"), F.lit(""))
         .when(F.substring(d, 2, 9) == F.lit("000000000"), F.lit(""))
         .otherwise(d)
    )


def zscore(value_col, mean_col, stdev_col):
    return F.when(
        stdev_col.isNull() | (stdev_col == 0) | value_col.isNull() | mean_col.isNull(),
        F.lit(0.0)
    ).otherwise((value_col - mean_col) / stdev_col)


ADDRESS_ABBREVIATIONS = {
    "SAINT": "ST", "STREET": "ST", "STR": "ST",
    "AVENUE": "AVE", "AV": "AVE",
    "ROAD": "RD", "BOULEVARD": "BLVD", "DRIVE": "DR",
    "LANE": "LN", "COURT": "CT", "PLACE": "PL",
    "TERRACE": "TER", "PARKWAY": "PKWY", "HIGHWAY": "HWY",
    "EXPRESSWAY": "EXPY", "SQUARE": "SQ", "CIRCLE": "CIR",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
}


@F.udf(T.StringType())
def normalize_address_udf(address):
    if address is None:
        return ""
    x = str(address).upper()
    x = x.replace(".", " ").replace(",", " ").replace("#", " ").replace("/", " ")
    x = re.sub(r"[^A-Z0-9\-\s]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    if not x:
        return ""
    out = []
    for token in x.split(" "):
        token = re.sub(r"([0-9]+)(ST|ND|RD|TH)$", r"\1", token)
        out.append(ADDRESS_ABBREVIATIONS.get(token, token))
    return " ".join(out)


def write_single_csv(df, output_file: str, latest_file: str = None):
    conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)

    tmp_dir = output_file + "_spark_tmp"
    tmp_path = jvm.org.apache.hadoop.fs.Path(tmp_dir)
    out_path = jvm.org.apache.hadoop.fs.Path(output_file)

    if fs.exists(tmp_path):
        fs.delete(tmp_path, True)
    if fs.exists(out_path):
        fs.delete(out_path, False)

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)

    part_path = None
    for st in fs.listStatus(tmp_path):
        name = st.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            part_path = st.getPath()
            break

    if part_path is None:
        raise RuntimeError(f"Nie znaleziono pliku part-*.csv w {tmp_dir}")

    fs.rename(part_path, out_path)
    fs.setReplication(out_path, 3)

    if latest_file:
        latest_path = jvm.org.apache.hadoop.fs.Path(latest_file)
        if fs.exists(latest_path):
            fs.delete(latest_path, False)
        jvm.org.apache.hadoop.fs.FileUtil.copy(fs, out_path, fs, latest_path, False, conf)
        fs.setReplication(latest_path, 3)

    fs.delete(tmp_path, True)
    print("Zapisano:", output_file)
    if latest_file:
        print("Zapisano latest:", latest_file)
# %% [markdown]
# ## Etap 1: TEMP1
# %%
def create_temp1(dohmh_path=DOHMH_INPUT, pluto_path=PLUTO_INPUT, results_dir=RESULTS_DIR, ts=RUN_TS):
    dohmh_raw = read_csv(dohmh_path)
    pluto_raw = read_csv(pluto_path)

    inspection_date = F.upper(F.trim(col_or_lit(dohmh_raw, "INSPECTION_DATE", "INSPECTION DATE")))
    score = safe_double(col_or_lit(dohmh_raw, "SCORE"))
    zip_code = first_five_digits_col(col_or_lit(dohmh_raw, "ZIPCODE", "ZIP CODE"))
    camis = only_digits_col(col_or_lit(dohmh_raw, "CAMIS"))
    building = F.trim(col_or_lit(dohmh_raw, "BUILDING"))
    street = F.trim(col_or_lit(dohmh_raw, "STREET"))
    boro = F.upper(F.trim(col_or_lit(dohmh_raw, "BORO", "BOROUGH")))
    cuisine = F.upper(F.trim(col_or_lit(dohmh_raw, "CUISINE_DESCRIPTION", "CUISINE DESCRIPTION")))
    bbl = norm_bbl_expr(col_or_lit(dohmh_raw, "BBL"))

    dohmh = (
        dohmh_raw
        .withColumn("ZIPCODE", zip_code)
        .withColumn("CAMIS", camis)
        .withColumn("SCORE", score)
        .withColumn("CUISINE_DESCRIPTION", F.when(cuisine == "", F.lit("UNKNOWN")).otherwise(cuisine))
        .withColumn("BUILDING", building)
        .withColumn("STREET", street)
        .withColumn("BORO", boro)
        .withColumn("BBL", bbl)
        .withColumn("ADDRESS", normalize_address_udf(F.concat_ws(" ", building, street)))
        .filter(~((inspection_date == "01/01/1900") | (inspection_date == "1900-01-01T00:00:00.000") | inspection_date.startswith("1900-01-01")))
        .filter(
            (F.col("ZIPCODE") != "") &
            (F.col("CAMIS") != "") &
            F.col("SCORE").isNotNull() &
            (F.col("BUILDING") != "") &
            (F.col("STREET") != "") &
            (F.col("BORO") != "")
        )
        .select("ZIPCODE", "CAMIS", "SCORE", "CUISINE_DESCRIPTION", "ADDRESS", "BORO", "BBL")
    )

    zip_stats = dohmh.groupBy("ZIPCODE").agg(
        F.countDistinct("CAMIS").alias("NUMBER_PER_ZIP"),
        F.avg("SCORE").alias("AVG_SCORE_ZIP")
    )

    boro_stats = dohmh.groupBy("BORO").agg(F.countDistinct("CAMIS").alias("NUMBER_PER_BORO"))

    boro_cd_stats = dohmh.groupBy("BORO", "CUISINE_DESCRIPTION").agg(
        F.avg("SCORE").alias("AVG_SCORE_BORO_CD")
    )

    pluto = (
        pluto_raw
        .withColumn("ADDRESS_NORM", normalize_address_udf(col_or_lit(pluto_raw, "ADDRESS")))
        .withColumn("ZIPCODE", first_five_digits_col(col_or_lit(pluto_raw, "POSTCODE", "ZIPCODE", "ZIP CODE")))
        .withColumn("BBL", norm_bbl_expr(col_or_lit(pluto_raw, "BBL")))
        .withColumn("LANDUSE_RAW", F.trim(col_or_lit(pluto_raw, "LANDUSE", "LAND USE")))
        .withColumn("YEARBUILT_RAW", safe_int(only_digits_col(col_or_lit(pluto_raw, "YEARBUILT", "YEAR BUILT"))))
        .filter(F.col("ADDRESS_NORM") != "")
    )

    pluto_bbl = pluto.filter(F.col("BBL") != "").groupBy(
        F.concat(F.lit("B|"), F.col("BBL")).alias("JOIN_KEY")
    ).agg(
        F.first(F.when(F.col("LANDUSE_RAW") != "", F.col("LANDUSE_RAW")), ignorenulls=True).alias("LANDUSE"),
        F.max("YEARBUILT_RAW").alias("YEARBUILT")
    )

    pluto_addr = pluto.filter((F.col("ZIPCODE") != "") & (F.col("ADDRESS_NORM") != "")).groupBy(
        F.concat_ws("|", F.lit("A"), F.col("ZIPCODE"), F.col("ADDRESS_NORM")).alias("JOIN_KEY")
    ).agg(
        F.first(F.when(F.col("LANDUSE_RAW") != "", F.col("LANDUSE_RAW")), ignorenulls=True).alias("LANDUSE"),
        F.max("YEARBUILT_RAW").alias("YEARBUILT")
    )

    pluto_join = pluto_bbl.unionByName(pluto_addr).dropDuplicates(["JOIN_KEY"])

    dohmh_join = dohmh.withColumn(
        "JOIN_KEY",
        F.when(F.col("BBL") != "", F.concat(F.lit("B|"), F.col("BBL")))
         .otherwise(F.concat_ws("|", F.lit("A"), F.col("ZIPCODE"), F.col("ADDRESS")))
    )

    temp1 = (
        dohmh_join
        .join(zip_stats, "ZIPCODE", "left")
        .join(boro_stats, "BORO", "left")
        .join(boro_cd_stats, ["BORO", "CUISINE_DESCRIPTION"], "left")
        .join(pluto_join, "JOIN_KEY", "left")
        .select(
            "ZIPCODE", "CAMIS", F.col("SCORE").cast("double").alias("SCORE"),
            "CUISINE_DESCRIPTION", "ADDRESS", "BORO",
            F.col("NUMBER_PER_ZIP").cast("long").alias("NUMBER_PER_ZIP"),
            F.col("NUMBER_PER_BORO").cast("long").alias("NUMBER_PER_BORO"),
            F.round("AVG_SCORE_ZIP", 4).alias("AVG_SCORE_ZIP"),
            F.round("AVG_SCORE_BORO_CD", 4).alias("AVG_SCORE_BORO_CD"),
            F.coalesce("LANDUSE", F.lit("")).alias("LANDUSE"),
            F.col("YEARBUILT").cast("int").alias("YEARBUILT")
        )
    )

    write_single_csv(temp1, f"{results_dir}/temp1_{ts}.csv", f"{results_dir}/temp1_latest.csv")
    return temp1
# %% [markdown]
# ## Etap 2: NYPD_ZIP
# %%
def parse_wkt_geometry(wkt: str):
    if not wkt:
        return []
    text = str(wkt).strip()
    upper = text.upper()
    if not (upper.startswith("POLYGON") or upper.startswith("MULTIPOLYGON")):
        return []
    nums = re.findall(r"-?\d+(?:\.\d+)?\s+-?\d+(?:\.\d+)?", text)
    points = []
    for pair in nums:
        lon, lat = [float(x) for x in pair.split()[:2]]
        points.append((lat, lon))
    return [points] if len(points) >= 3 else []


def parse_geojson_geometry(text: str):
    if not text:
        return []
    try:
        obj = json.loads(text)
    except Exception:
        return []
    geom_type = str(obj.get("type", "")).upper()
    coords = obj.get("coordinates")
    if not coords:
        return []
    polygons = []
    if geom_type == "POLYGON":
        outer = coords[0] if coords else []
        polygons.append([(float(lat), float(lon)) for lon, lat in outer])
    elif geom_type == "MULTIPOLYGON":
        for poly in coords:
            outer = poly[0] if poly else []
            polygons.append([(float(lat), float(lon)) for lon, lat in outer])
    return [p for p in polygons if len(p) >= 3]


def parse_any_geometry(text: str):
    if not text:
        return []
    u = str(text).strip().upper()
    if u.startswith("POLYGON") or u.startswith("MULTIPOLYGON"):
        return parse_wkt_geometry(text)
    if "COORDINATES" in u:
        return parse_geojson_geometry(text)
    return []


def point_in_ring(lat: float, lon: float, ring):
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        lati, loni = ring[i]
        latj, lonj = ring[j]
        crosses = ((loni > lon) != (lonj > lon)) and (
            lat < (latj - lati) * (lon - loni) / ((lonj - loni) or 1e-12) + lati
        )
        if crosses:
            inside = not inside
        j = i
    return inside


def create_nypd_zip(nypd_path=NYPD_INPUT, modzcta_path=MODZCTA_INPUT, results_dir=RESULTS_DIR, ts=RUN_TS):
    nypd = read_csv(nypd_path)
    modzcta = read_csv(modzcta_path)

    zip_candidates = ["MODZCTA", "ZCTA", "ZIPCODE", "ZIP_CODE", "POSTCODE", "ZIP"]
    geom_candidates = ["THE_GEOM", "GEOMETRY", "GEOM", "SHAPE", "WKT", "NEW_GEOREFERENCED_COLUMN", "GEOCODED_COLUMN"]

    zip_col = next((c for c in zip_candidates if c in modzcta.columns), None)
    geom_col = next((c for c in geom_candidates if c in modzcta.columns), None)
    if zip_col is None or geom_col is None:
        raise ValueError(f"Nie znaleziono kolumn ZIP/geometrii w MODZCTA. Kolumny: {modzcta.columns}")

    areas = []
    for row in modzcta.select(zip_col, geom_col).collect():
        zip_code = re.sub(r"[^0-9]", "", str(row[zip_col] or ""))[:5]
        polygons = parse_any_geometry(row[geom_col])
        if zip_code and polygons:
            areas.append((zip_code, polygons))
    if not areas:
        raise ValueError("Nie wczytano żadnych poligonów MODZCTA.")

    areas_bc = spark.sparkContext.broadcast(areas)

    @F.udf(T.StringType())
    def find_zip_udf(lat, lon):
        if lat is None or lon is None:
            return ""
        try:
            lat = float(lat)
            lon = float(lon)
        except Exception:
            return ""
        if not (40.0 <= lat <= 41.2 and -75.2 <= lon <= -72.8):
            return ""
        for zip_code, polygons in areas_bc.value:
            for polygon in polygons:
                if point_in_ring(lat, lon, polygon):
                    return zip_code
        return ""

    lat = safe_double(col_or_lit(nypd, "LATITUDE"))
    lon = safe_double(col_or_lit(nypd, "LONGITUDE"))

    nypd_zip = (
        nypd
        .withColumn("CMPLNT_NUM", F.trim(col_or_lit(nypd, "CMPLNT_NUM")))
        .withColumn("OFNS_DESC", F.upper(F.trim(col_or_lit(nypd, "OFNS_DESC"))))
        .withColumn("BORO_NM", F.upper(F.trim(col_or_lit(nypd, "BORO_NM"))))
        .withColumn("LATITUDE", lat)
        .withColumn("LONGITUDE", lon)
        .withColumn("ZIPCODE", find_zip_udf("LATITUDE", "LONGITUDE"))
        .filter((F.col("CMPLNT_NUM") != "") & (F.col("OFNS_DESC") != "") & (F.col("ZIPCODE") != ""))
        .select(
            "CMPLNT_NUM", "ZIPCODE", "OFNS_DESC", "BORO_NM",
            F.round("LATITUDE", 6).alias("LATITUDE"),
            F.round("LONGITUDE", 6).alias("LONGITUDE")
        )
    )

    write_single_csv(nypd_zip, f"{results_dir}/nypd_zip_{ts}.csv", f"{results_dir}/nypd_zip_latest.csv")
    return nypd_zip
# %% [markdown]
# ## Etap 3: TEMP2
# %%
def create_temp2(temp1_path=None, nypd_zip_path=None, results_dir=RESULTS_DIR, ts=RUN_TS):
    temp1_path = temp1_path or f"{results_dir}/temp1_latest.csv"
    nypd_zip_path = nypd_zip_path or f"{results_dir}/nypd_zip_latest.csv"

    temp1 = read_csv(temp1_path)
    nypd_zip = read_csv(nypd_zip_path)

    temp1 = (
        temp1
        .withColumn("ZIPCODE", first_five_digits_col(col_or_lit(temp1, "ZIPCODE")))
        .withColumn("BORO", F.upper(F.trim(col_or_lit(temp1, "BORO"))))
        .withColumn("SCORE", safe_double(col_or_lit(temp1, "SCORE")))
        .withColumn("NUMBER_PER_ZIP", safe_double(col_or_lit(temp1, "NUMBER_PER_ZIP")))
        .withColumn("NUMBER_PER_BORO", safe_double(col_or_lit(temp1, "NUMBER_PER_BORO")))
        .withColumn("AVG_SCORE_ZIP", safe_double(col_or_lit(temp1, "AVG_SCORE_ZIP")))
        .withColumn("AVG_SCORE_BORO_CD", safe_double(col_or_lit(temp1, "AVG_SCORE_BORO_CD")))
        .withColumn("YEARBUILT", safe_int(col_or_lit(temp1, "YEARBUILT")))
    )

    zip_unique = temp1.select("ZIPCODE", "NUMBER_PER_ZIP", "AVG_SCORE_ZIP").dropDuplicates(["ZIPCODE"])
    boro_unique = temp1.select("BORO", "NUMBER_PER_BORO").dropDuplicates(["BORO"])

    stdev_per_zip = zip_unique.agg(F.stddev_pop("NUMBER_PER_ZIP").alias("v")).first()["v"] or 0.0
    stdev_per_boro = boro_unique.agg(F.stddev_pop("NUMBER_PER_BORO").alias("v")).first()["v"] or 0.0

    crime_base = (
        nypd_zip
        .withColumn("ZIPCODE", first_five_digits_col(col_or_lit(nypd_zip, "ZIPCODE")))
        .withColumn("CMPLNT_NUM", F.trim(col_or_lit(nypd_zip, "CMPLNT_NUM")))
        .withColumn("OFNS_DESC", F.upper(F.trim(col_or_lit(nypd_zip, "OFNS_DESC"))))
        .filter((F.col("ZIPCODE") != "") & (F.col("CMPLNT_NUM") != "") & (F.col("OFNS_DESC") != ""))
        .dropDuplicates(["ZIPCODE", "CMPLNT_NUM"])
    )

    crime_count = crime_base.groupBy("ZIPCODE").agg(F.countDistinct("CMPLNT_NUM").alias("COUNT_CRIME_PER_ZIP"))

    offense_counts = crime_base.groupBy("ZIPCODE", "OFNS_DESC").agg(F.count("*").alias("OFFENSE_COUNT"))
    w_dom = Window.partitionBy("ZIPCODE").orderBy(F.desc("OFFENSE_COUNT"), F.asc("OFNS_DESC"))
    dominant = (
        offense_counts.withColumn("rn", F.row_number().over(w_dom))
        .filter(F.col("rn") == 1)
        .select("ZIPCODE", F.col("OFNS_DESC").alias("DOMINANT_CRIME_TYPE"))
    )

    crime_aligned = (
        temp1.select("ZIPCODE").dropDuplicates()
        .join(crime_count.join(dominant, "ZIPCODE", "left"), "ZIPCODE", "left")
        .fillna({"COUNT_CRIME_PER_ZIP": 0, "DOMINANT_CRIME_TYPE": ""})
    )

    avg_crime = crime_aligned.agg(F.avg("COUNT_CRIME_PER_ZIP").alias("v")).first()["v"] or 0.0
    stdev_crime = crime_aligned.agg(F.stddev_pop("COUNT_CRIME_PER_ZIP").alias("v")).first()["v"] or 0.0

    score_stats = zip_unique.agg(
        F.avg("AVG_SCORE_ZIP").alias("mean_score"),
        F.stddev_pop("AVG_SCORE_ZIP").alias("stdev_score")
    ).first()
    mean_avg_score_zip = score_stats["mean_score"] or 0.0
    stdev_avg_score_zip = score_stats["stdev_score"] or 0.0

    temp2 = (
        temp1
        .join(crime_aligned, "ZIPCODE", "left")
        .withColumn("STDEV_PER_ZIP", F.lit(stdev_per_zip))
        .withColumn("STDEV_PER_BORO", F.lit(stdev_per_boro))
        .withColumn(
            "RESTAURANT_DENSITY_QUALITY_INDEX",
            F.when(F.col("AVG_SCORE_ZIP").isNull() | (F.col("AVG_SCORE_ZIP") == 0), F.lit(None).cast("double"))
             .otherwise(F.col("NUMBER_PER_ZIP") / F.col("AVG_SCORE_ZIP"))
        )
        .withColumn(
            "BUILDING_AGE",
            F.when((F.col("YEARBUILT") > 0) & (F.col("YEARBUILT") <= CURRENT_YEAR), F.lit(CURRENT_YEAR) - F.col("YEARBUILT"))
        )
        .withColumn("AVG_CRIME_PER_ZIP", F.lit(avg_crime))
        .withColumn(
            "CRIME_INSPECTION_RISK_SCORE",
            zscore(F.col("COUNT_CRIME_PER_ZIP"), F.lit(avg_crime), F.lit(stdev_crime)) +
            zscore(F.col("AVG_SCORE_ZIP"), F.lit(mean_avg_score_zip), F.lit(stdev_avg_score_zip))
        )
        .select(
            "ZIPCODE", "CAMIS", "SCORE", "CUISINE_DESCRIPTION", "ADDRESS", "BORO",
            F.col("NUMBER_PER_ZIP").cast("long").alias("NUMBER_PER_ZIP"),
            F.round("STDEV_PER_ZIP", 4).alias("STDEV_PER_ZIP"),
            F.col("NUMBER_PER_BORO").cast("long").alias("NUMBER_PER_BORO"),
            F.round("STDEV_PER_BORO", 4).alias("STDEV_PER_BORO"),
            F.round("AVG_SCORE_ZIP", 4).alias("AVG_SCORE_ZIP"),
            F.round("AVG_SCORE_BORO_CD", 4).alias("AVG_SCORE_BORO_CD"),
            F.round("RESTAURANT_DENSITY_QUALITY_INDEX", 4).alias("RESTAURANT_DENSITY_QUALITY_INDEX"),
            "LANDUSE", "BUILDING_AGE", "YEARBUILT",
            F.round("AVG_CRIME_PER_ZIP", 4).alias("AVG_CRIME_PER_ZIP"),
            F.col("COUNT_CRIME_PER_ZIP").cast("long").alias("COUNT_CRIME_PER_ZIP"),
            "DOMINANT_CRIME_TYPE",
            F.round("CRIME_INSPECTION_RISK_SCORE", 4).alias("CRIME_INSPECTION_RISK_SCORE")
        )
    )

    write_single_csv(temp2, f"{results_dir}/temp2_{ts}.csv", f"{results_dir}/temp2_latest.csv")
    return temp2
# %% [markdown]
# ## Etap 4: TEMP3
# %%
def create_temp3(temp2_path=None, results_dir=RESULTS_DIR, ts=RUN_TS):
    temp2_path = temp2_path or f"{results_dir}/temp2_latest.csv"
    temp2 = read_csv(temp2_path)

    temp2 = (
        temp2
        .withColumn("ZIPCODE", first_five_digits_col(col_or_lit(temp2, "ZIPCODE")))
        .withColumn("BORO", F.upper(F.trim(col_or_lit(temp2, "BORO"))))
        .withColumn("CUISINE_DESCRIPTION", F.upper(F.trim(col_or_lit(temp2, "CUISINE_DESCRIPTION", "CUISINE DESCRIPTION"))))
        .withColumn("SCORE", safe_double(col_or_lit(temp2, "SCORE")))
        .withColumn("AVG_SCORE_ZIP", safe_double(col_or_lit(temp2, "AVG_SCORE_ZIP")))
        .withColumn("AVG_SCORE_BORO_CD", safe_double(col_or_lit(temp2, "AVG_SCORE_BORO_CD")))
        .withColumn("AVG_CRIME_PER_ZIP", safe_double(col_or_lit(temp2, "AVG_CRIME_PER_ZIP")))
        .withColumn("COUNT_CRIME_PER_ZIP", safe_double(col_or_lit(temp2, "COUNT_CRIME_PER_ZIP")))
    )

    stdev_score_zip = temp2.groupBy("ZIPCODE").agg(F.stddev_pop("SCORE").alias("STDEV_SCORE_ZIP"))
    stdev_score_boro_cd = temp2.groupBy("BORO", "CUISINE_DESCRIPTION").agg(F.stddev_pop("SCORE").alias("STDEV_SCORE_BORO_CD"))
    stdev_crime = (
        temp2.select("ZIPCODE", "COUNT_CRIME_PER_ZIP")
        .dropDuplicates(["ZIPCODE"])
        .agg(F.stddev_pop("COUNT_CRIME_PER_ZIP").alias("v"))
        .first()["v"] or 0.0
    )

    temp3 = (
        temp2
        .join(stdev_score_zip, "ZIPCODE", "left")
        .join(stdev_score_boro_cd, ["BORO", "CUISINE_DESCRIPTION"], "left")
        .withColumn("NORM_SCORE_ZIP", zscore(F.col("SCORE"), F.col("AVG_SCORE_ZIP"), F.col("STDEV_SCORE_ZIP")))
        .withColumn("NORM_CRIME_ZIP", zscore(F.col("COUNT_CRIME_PER_ZIP"), F.col("AVG_CRIME_PER_ZIP"), F.lit(stdev_crime)))
        .withColumn("CRIME_INSPECTION_RISK_SCORE", F.col("NORM_SCORE_ZIP") + F.col("NORM_CRIME_ZIP"))
        .withColumn("CUISINE_RELATIVE_SCORE", zscore(F.col("SCORE"), F.col("AVG_SCORE_BORO_CD"), F.col("STDEV_SCORE_BORO_CD")))
        .select(
            "CAMIS", "BORO", "ZIPCODE", "ADDRESS", "CUISINE_DESCRIPTION",
            "YEARBUILT", "LANDUSE",
            F.round("CRIME_INSPECTION_RISK_SCORE", 4).alias("CRIME_INSPECTION_RISK_SCORE"),
            F.col("BUILDING_AGE").alias("BUILDING_AGE_SCORE"),
            "RESTAURANT_DENSITY_QUALITY_INDEX",
            F.round("CUISINE_RELATIVE_SCORE", 4).alias("CUISINE_RELATIVE_SCORE"),
            "DOMINANT_CRIME_TYPE"
        )
    )

    write_single_csv(temp3, f"{results_dir}/temp3_{ts}.csv", f"{results_dir}/temp3_latest.csv")
    return temp3
# %% [markdown]
# ## Uruchomienie całego pipeline
# 
# Ustaw `RUN_FULL_PIPELINE = True`, aby wykonać wszystkie etapy.
# %%
RUN_FULL_PIPELINE = True

if RUN_FULL_PIPELINE:
    temp1_df = create_temp1()
    nypd_zip_df = create_nypd_zip()
    temp2_df = create_temp2()
    temp3_df = create_temp3()

    print("TEMP1:", temp1_df.count())
    print("NYPD_ZIP:", nypd_zip_df.count())
    print("TEMP2:", temp2_df.count())
    print("TEMP3:", temp3_df.count())
else:
    print("RUN_FULL_PIPELINE = False. Ustaw True, aby uruchomić wszystkie etapy.")
# %% [markdown]
# ## Szybka weryfikacja
# %%
temp3_latest_path = f"{RESULTS_DIR}/temp3_latest.csv"

try:
    temp3_check = read_csv(temp3_latest_path)
    print("Liczba rekordów temp3_latest:", temp3_check.count())
    temp3_check.printSchema()
    temp3_check.show(10, truncate=False)
except Exception as exc:
    print("Nie udało się odczytać temp3_latest. Najpierw uruchom pipeline.")
    print(exc)

# %% [markdown]
# # Databricks Free Edition: pipeline DOHMH + PLUTO + NYPD
# 
# Notebook jest wersją pipeline Spark dostosowaną do Databricks Free Edition / serverless compute.
# 
# Główne różnice względem wersji uruchamianej lokalnie przez `spark-submit`:
# - wejścia i wyjścia są w Unity Catalog Volume: `/Volumes/workspace/default/project_files`,
# - nie używamy `spark.sparkContext`, `df.rdd`, `_jvm`, `_jsc` ani ręcznego scalania plików przez Hadoop FileSystem,
# - wyniki Spark są zapisywane jako katalogi CSV, np. `temp3_latest.csv` jest katalogiem z plikami `part-*`,
# - etap `NYPD_ZIP` zachowuje przypisanie ZIP przez point-in-polygon, ale używa Databricks-compatible Python UDF z indeksem przestrzennym.
# 
# Etapy:
# 1. `TEMP1`: czyszczenie DOHMH, agregacje i połączenie z PLUTO.
# 2. `NYPD_ZIP`: przypisanie rekordów NYPD do ZIP/MODZCTA na podstawie współrzędnych.
# 3. `TEMP2`: połączenie `TEMP1` z agregatami przestępczości.
# 4. `TEMP3`: finalny zbiór wskaźników.
# %%
from time import perf_counter
from datetime import datetime
import json
import math
import re

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import types as T

print("Spark version:", spark.version)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
CURRENT_YEAR = datetime.now().year

print("Timestamp uruchomienia:", RUN_TS)
# %% [markdown]
# ## Konfiguracja ścieżek w Databricks Volume
# 
# Pliki wejściowe powinny znajdować się w:
# 
# `Catalog: workspace`  
# `Schema: default`  
# `Volume: project_files`
# %%
BASE_VOLUME = "/Volumes/workspace/default/project_files"

DOHMH_INPUT = f"{BASE_VOLUME}/DOHMH_latest.csv"
PLUTO_INPUT = f"{BASE_VOLUME}/PLUTO.csv"
NYPD_INPUT = f"{BASE_VOLUME}/NYPD.csv"
MODZCTA_INPUT = f"{BASE_VOLUME}/MODZCTA.csv"

RESULTS_DIR = f"{BASE_VOLUME}/results"

print("DOHMH_INPUT:", DOHMH_INPUT)
print("PLUTO_INPUT:", PLUTO_INPUT)
print("NYPD_INPUT:", NYPD_INPUT)
print("MODZCTA_INPUT:", MODZCTA_INPUT)
print("RESULTS_DIR:", RESULTS_DIR)
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

def format_duration(seconds: float) -> str:
    total_ms = int(round(seconds * 1000))
    ms = total_ms % 1000
    total_s = total_ms // 1000
    s = total_s % 60
    m = (total_s // 60) % 60
    h = total_s // 3600
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

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

def write_csv_result(df, base_name: str, results_dir: str = RESULTS_DIR, ts: str = RUN_TS):
    """
    Zapisuje wynik w Databricks Volume jako katalog CSV.

    W Databricks serverless nie używamy Hadoop FileSystem przez _jvm/_jsc.
    Dlatego wynik zapisywany jest jako katalog z plikami part-*.
    Spark potrafi później czytać taki katalog tak samo jak pojedynczy CSV.
    """
    versioned_path = f"{results_dir}/{base_name}_{ts}.csv"
    latest_path = f"{results_dir}/{base_name}_latest.csv"

    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(versioned_path)
    )

    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(latest_path)
    )

    print("Zapisano wersję:", versioned_path)
    print("Zapisano latest:", latest_path)

    return versioned_path, latest_path
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

    boro_stats = dohmh.groupBy("BORO").agg(
        F.countDistinct("CAMIS").alias("NUMBER_PER_BORO")
    )

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

    write_csv_result(temp1, "temp1", results_dir, ts)
    return read_csv(f"{results_dir}/temp1_latest.csv")
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

def ring_bbox(ring):
    lats = [p[0] for p in ring]
    lons = [p[1] for p in ring]
    return (min(lats), max(lats), min(lons), max(lons))

def build_spatial_grid(areas, grid_size=0.01):
    grid = {}
    polygon_count = 0

    for zip_code, polygons in areas:
        for ring in polygons:
            if len(ring) < 3:
                continue

            bbox = ring_bbox(ring)
            min_lat, max_lat, min_lon, max_lon = bbox

            min_i = math.floor(min_lat / grid_size)
            max_i = math.floor(max_lat / grid_size)
            min_j = math.floor(min_lon / grid_size)
            max_j = math.floor(max_lon / grid_size)

            payload = (zip_code, bbox, ring)

            for i in range(min_i, max_i + 1):
                for j in range(min_j, max_j + 1):
                    grid.setdefault((i, j), []).append(payload)

            polygon_count += 1

    return grid, polygon_count

def grid_key(lat, lon, grid_size=0.01):
    return (math.floor(lat / grid_size), math.floor(lon / grid_size))

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
        polygons = [p for p in polygons if len(p) >= 3]
        if zip_code and polygons:
            areas.append((zip_code, polygons))

    if not areas:
        raise ValueError("Nie wczytano żadnych poligonów MODZCTA.")

    grid_size = 0.01
    spatial_grid, polygon_count = build_spatial_grid(areas, grid_size=grid_size)

    print(f"MODZCTA: obszary ZIP={len(areas)}, poligony={polygon_count}, komórki siatki={len(spatial_grid)}")

    def find_zip_local(lat, lon):
        if lat is None or lon is None:
            return ""
        try:
            lat_value = float(lat)
            lon_value = float(lon)
        except Exception:
            return ""

        candidates = spatial_grid.get(grid_key(lat_value, lon_value, grid_size), [])

        for zip_code, bbox, ring in candidates:
            min_lat, max_lat, min_lon, max_lon = bbox

            if lat_value < min_lat or lat_value > max_lat or lon_value < min_lon or lon_value > max_lon:
                continue

            if point_in_ring(lat_value, lon_value, ring):
                return zip_code

        return ""

    find_zip_udf = F.udf(find_zip_local, T.StringType())

    lat = safe_double(col_or_lit(nypd, "LATITUDE"))
    lon = safe_double(col_or_lit(nypd, "LONGITUDE"))

    nypd_prepared = (
        nypd
        .withColumn("CMPLNT_NUM", F.trim(col_or_lit(nypd, "CMPLNT_NUM")))
        .withColumn("OFNS_DESC", F.upper(F.trim(col_or_lit(nypd, "OFNS_DESC"))))
        .withColumn("BORO_NM", F.upper(F.trim(col_or_lit(nypd, "BORO_NM"))))
        .withColumn("LATITUDE", lat)
        .withColumn("LONGITUDE", lon)
        .filter(
            (F.col("CMPLNT_NUM") != "") &
            (F.col("OFNS_DESC") != "") &
            F.col("LATITUDE").isNotNull() &
            F.col("LONGITUDE").isNotNull() &
            (F.col("LATITUDE") >= 40.0) &
            (F.col("LATITUDE") <= 41.2) &
            (F.col("LONGITUDE") >= -75.2) &
            (F.col("LONGITUDE") <= -72.8)
        )
        .select("CMPLNT_NUM", "OFNS_DESC", "BORO_NM", "LATITUDE", "LONGITUDE")
    )

    nypd_zip = (
        nypd_prepared
        .withColumn("ZIPCODE", find_zip_udf("LATITUDE", "LONGITUDE"))
        .filter(F.col("ZIPCODE") != "")
        .select(
            "CMPLNT_NUM",
            "ZIPCODE",
            "OFNS_DESC",
            "BORO_NM",
            F.round("LATITUDE", 6).alias("LATITUDE"),
            F.round("LONGITUDE", 6).alias("LONGITUDE")
        )
        .cache()
    )

    rows_before_write = nypd_zip.count()
    print(f"NYPD_ZIP rows przed zapisem: {rows_before_write}")

    write_csv_result(nypd_zip, "nypd_zip", results_dir, ts)

    nypd_zip.unpersist()
    return read_csv(f"{results_dir}/nypd_zip_latest.csv")
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

    crime_count = crime_base.groupBy("ZIPCODE").agg(
        F.countDistinct("CMPLNT_NUM").alias("COUNT_CRIME_PER_ZIP")
    )

    offense_counts = crime_base.groupBy("ZIPCODE", "OFNS_DESC").agg(
        F.count("*").alias("OFFENSE_COUNT")
    )

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

    write_csv_result(temp2, "temp2", results_dir, ts)
    return read_csv(f"{results_dir}/temp2_latest.csv")
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

    stdev_score_zip = temp2.groupBy("ZIPCODE").agg(
        F.stddev_pop("SCORE").alias("STDEV_SCORE_ZIP")
    )

    stdev_score_boro_cd = temp2.groupBy("BORO", "CUISINE_DESCRIPTION").agg(
        F.stddev_pop("SCORE").alias("STDEV_SCORE_BORO_CD")
    )

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

    write_csv_result(temp3, "temp3", results_dir, ts)
    return read_csv(f"{results_dir}/temp3_latest.csv")
# %% [markdown]
# ## Uruchomienie pipeline z pomiarem czasu
# %%
def run_timed_stage(stage_name: str, stage_function):
    print(f"\n===== START ETAPU: {stage_name} =====")
    start = perf_counter()

    try:
        df = stage_function()
        rows = df.count()
        elapsed = perf_counter() - start

        print("STATUS: OK")
        print(f"{stage_name} rows: {rows}")
        print(f"Czas trwania etapu {stage_name}: {format_duration(elapsed)} ({elapsed:.3f} s)")
        print(f"===== KONIEC ETAPU: {stage_name} =====\n")

        return df, elapsed, rows, "OK"

    except Exception as exc:
        elapsed = perf_counter() - start

        print("STATUS: ERROR")
        print(f"Czas do błędu etapu {stage_name}: {format_duration(elapsed)} ({elapsed:.3f} s)")
        print("Typ błędu:", type(exc).__name__)
        print("Treść błędu:")
        print(str(exc)[:3000])
        print(f"===== KONIEC ETAPU Z BŁĘDEM: {stage_name} =====\n")

        raise

def print_timing_summary(stage_results):
    print("\n===== PODSUMOWANIE CZASÓW PIPELINE =====")
    total = 0.0
    for stage_name, elapsed, rows, status in stage_results:
        total += elapsed
        print(f"{stage_name}: {format_duration(elapsed)} ({elapsed:.3f} s), rows={rows}, status={status}")
    print(f"Łączny czas etapów: {format_duration(total)} ({total:.3f} s)")
    print("========================================\n")

RUN_FULL_PIPELINE = False

if RUN_FULL_PIPELINE:
    pipeline_start = perf_counter()
    stage_results = []

    temp1_df, elapsed, rows, status = run_timed_stage("TEMP1", create_temp1)
    stage_results.append(("TEMP1", elapsed, rows, status))

    nypd_zip_df, elapsed, rows, status = run_timed_stage("NYPD_ZIP", create_nypd_zip)
    stage_results.append(("NYPD_ZIP", elapsed, rows, status))

    temp2_df, elapsed, rows, status = run_timed_stage("TEMP2", create_temp2)
    stage_results.append(("TEMP2", elapsed, rows, status))

    temp3_df, elapsed, rows, status = run_timed_stage("TEMP3", create_temp3)
    stage_results.append(("TEMP3", elapsed, rows, status))

    print_timing_summary(stage_results)

    pipeline_elapsed = perf_counter() - pipeline_start
    print(f"Całkowity czas uruchomienia pipeline: {format_duration(pipeline_elapsed)} ({pipeline_elapsed:.3f} s)")
else:
    print("RUN_FULL_PIPELINE = False. Ustaw True, aby uruchomić wszystkie etapy.")
# %% [markdown]
# ## Szybka weryfikacja wyniku TEMP3
# %%
temp3_latest_path = f"{RESULTS_DIR}/temp3_latest.csv"

try:
    temp3_check = read_csv(temp3_latest_path)

    print("Liczba rekordów temp3_latest:", temp3_check.count())
    temp3_check.printSchema()

    display(temp3_check.limit(10))

except Exception as exc:
    print("Nie udało się odczytać temp3_latest. Najpierw uruchom pipeline.")
    print("Typ błędu:", type(exc).__name__)
    print(str(exc)[:3000])
# %% [markdown]
# ## Prosta agregacja kontrolna po BORO
# %%
try:
    temp3_check = read_csv(f"{RESULTS_DIR}/temp3_latest.csv")

    summary_df = (
        temp3_check
        .groupBy("BORO")
        .agg(
            F.count("*").alias("RECORDS"),
            F.round(F.avg("CRIME_INSPECTION_RISK_SCORE"), 4).alias("AVG_RISK_SCORE"),
            F.round(F.avg("CUISINE_RELATIVE_SCORE"), 4).alias("AVG_CUISINE_RELATIVE_SCORE"),
            F.round(F.avg("RESTAURANT_DENSITY_QUALITY_INDEX"), 4).alias("AVG_RESTAURANT_DENSITY_QUALITY_INDEX")
        )
        .orderBy("BORO")
    )

    display(summary_df)

except Exception as exc:
    print("Nie udało się wykonać agregacji. Najpierw uruchom pipeline.")
    print("Typ błędu:", type(exc).__name__)
    print(str(exc)[:3000])
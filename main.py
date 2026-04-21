import requests
import pandas as pd
import time
from datetime import datetime
import os
import shutil
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

RUN_ID = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"pipeline_{RUN_ID}.log")


# =========================
# LOGGING
# =========================
def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] [{level}] {message}"
    print(full_msg)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")


# =========================
# HTTP (z retry)
# =========================
def safe_request(url, params=None, retries=3, timeout=15, stream=False):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout, stream=stream)
            r.raise_for_status()
            return r
        except Exception as e:
            log(f"Błąd requestu (próba {attempt+1}): {e}", "WARN")
            time.sleep(2)

    raise Exception("Nie udało się pobrać danych po kilku próbach")


# =========================
# DOWNLOAD z progress
# =========================
def download_with_progress(url, filepath, desc):
    r = safe_request(url, stream=True)

    total_size = int(r.headers.get("content-length", 0))

    with open(filepath, "wb") as f, tqdm(
        desc=desc,
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                bar.update(len(chunk))


# =========================
# HDFS
# =========================
def upload_to_hdfs(local_path, hdfs_path):
    filename = os.path.basename(local_path)

    log(f"Upload do HDFS: {filename}")

    os.system(f"hdfs dfs -mkdir -p {hdfs_path}")
    os.system(f"hdfs dfs -rm -f {hdfs_path}/{filename}")
    os.system(f"hdfs dfs -put {local_path} {hdfs_path}/")
    os.system(f"hdfs dfs -setrep -w 3 {hdfs_path}/{filename}")

    log(f"Wgrano do HDFS: {hdfs_path}/{filename} (3 repliki)")


# =========================
# STATIC CSV
# =========================
def download_static_csv(url, filename):
    filepath = os.path.join(DATA_DIR, filename)

    if os.path.exists(filepath):
        log(f"Plik już istnieje, pomijam: {filename}")
        return

    log(f"Pobieranie: {filename}")
    download_with_progress(url, filepath, filename)

    size = os.path.getsize(filepath)
    log(f"Pobrano {filename} ({size} bajtów)")

    upload_to_hdfs(filepath, "/datasets/static")


# =========================
# LOAD LAST CAMIS
# =========================
def load_last_camis(filepath, n=10):
    df = pd.read_csv(filepath, low_memory=False, dtype=str)
    df.columns = df.columns.str.lower()

    if "camis" not in df.columns:
        raise Exception("Brak kolumny 'camis' w CSV")

    return df["camis"].dropna().astype(str).head(n).tolist()


# =========================
# FIND MATCH
# =========================
def find_match(new_data, last_camis):
    last_camis_set = set(str(x) for x in last_camis)

    for i, row in enumerate(new_data):
        camis = str(row.get("camis"))
        if camis in last_camis_set:
            return i

    return None


# =========================
# DOHMH
# =========================
def process_dohmh():
    base_csv = os.path.join(DATA_DIR, "DOHMH_latest.csv")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_url = "https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD"
    json_url = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"

    limits = [5000, 10000, 20000, 50000]

    # =========================
    # FULL LOAD (brak pliku)
    # =========================
    if not os.path.exists(base_csv):
        log("Brak pliku DOHMH → FULL LOAD")

        full_path = os.path.join(DATA_DIR, f"DOHMH_{timestamp}.csv")

        download_with_progress(csv_url, full_path, "DOHMH FULL")

        # kopiujemy do latest (zostawiając oryginał!)
        shutil.copy(full_path, base_csv)

        upload_to_hdfs(full_path, "/datasets/dohmh")
        upload_to_hdfs(base_csv, "/datasets/dohmh")
        return

    # =========================
    # INCREMENTAL
    # =========================
    last_camis = load_last_camis(base_csv, 100)

    for limit in tqdm(limits, desc="Szukanie matcha", unit="próba", leave=False):
        log(f"Limit {limit}")

        r = safe_request(json_url, params={"$limit": limit})
        new_data = r.json()

        match_index = find_match(new_data, last_camis)

        if match_index is not None:
            log(f"Znaleziono match: {match_index}")

            new_records = new_data[:match_index]

            if not new_records:
                log("Brak nowych danych")
                return

            df_old = pd.read_csv(base_csv, dtype=str)
            df_new = pd.DataFrame(new_records)

            mapping = {c.lower(): c for c in df_old.columns}
            df_new = df_new.rename(columns=lambda x: mapping.get(x.lower(), x))
            df_new = df_new.reindex(columns=df_old.columns)

            combined = pd.concat([df_new, df_old], ignore_index=True)

            new_file = os.path.join(DATA_DIR, f"DOHMH_{timestamp}.csv")

            combined.to_csv(new_file, index=False)
            combined.to_csv(base_csv, index=False)

            log(f"Nowe rekordy: {len(new_records)}")

            upload_to_hdfs(new_file, "/datasets/dohmh")
            upload_to_hdfs(base_csv, "/datasets/dohmh")

            return

    # =========================
    # FALLBACK FULL LOAD
    # =========================
    log("Brak matcha → FULL LOAD", "WARN")

    full_path = os.path.join(DATA_DIR, f"DOHMH_{timestamp}.csv")

    download_with_progress(csv_url, full_path, "DOHMH FULL")

    shutil.copy(full_path, base_csv)

    upload_to_hdfs(full_path, "/datasets/dohmh")
    upload_to_hdfs(base_csv, "/datasets/dohmh")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    start = time.time()

    try:
        log("START PIPELINE")

        download_static_csv(
            "https://data.cityofnewyork.us/api/views/64uk-42ks/rows.csv?accessType=DOWNLOAD",
            "PLUTO.csv",
        )

        download_static_csv(
            "https://data.cityofnewyork.us/api/views/5uac-w243/rows.csv?accessType=DOWNLOAD",
            "NYPD.csv",
        )

        process_dohmh()

        duration = time.time() - start
        log(f"ZAKOŃCZONO SUKCESEM w {round(duration, 2)}s", "SUCCESS")

    except Exception as e:
        log(f"BŁĄD KRYTYCZNY: {e}", "ERROR")
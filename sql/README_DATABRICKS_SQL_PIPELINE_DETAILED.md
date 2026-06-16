# README — Databricks SQL Pipeline dla projektu DOHMH + PLUTO + NYPD

Ten dokument opisuje działanie notebooka `databricks_sql_pipeline_project.ipynb`, który uruchamia pipeline przetwarzania danych w **Databricks Free Edition / serverless compute** z wykorzystaniem **tabel Delta w Unity Catalog** oraz transformacji zapisanych jako **SQL**.

Dokument jest napisany tak, aby osoba, która pierwszy raz widzi ten kod, mogła zrozumieć:

- po co powstała wersja SQL,
- jak przenieść pliki CSV do bazy danych Databricks,
- czym różni się podejście SQL od wcześniejszego podejścia DataFrame,
- jak działa każda część notebooka,
- jakie tabele wejściowe i wynikowe powstają,
- jak uruchomić pipeline na danych testowych i pełnych,
- jak interpretować wyniki oraz najczęstsze błędy.

---

## 1. Cel notebooka

Notebook `databricks_sql_pipeline_project.ipynb` jest SQL-ową wersją wcześniejszego pipeline Spark/DataFrame.

Pipeline przetwarza dane z czterech źródeł:

1. **DOHMH** — dane o inspekcjach restauracji,
2. **PLUTO** — dane budynkowe i przestrzenne,
3. **NYPD** — dane o zdarzeniach policyjnych,
4. **MODZCTA** — geometrie obszarów ZIP/MODZCTA.

Celem pipeline jest utworzenie finalnej tabeli `TEMP3`, która zawiera m.in.:

- identyfikator restauracji `CAMIS`,
- dzielnicę `BORO`,
- kod ZIP,
- adres,
- typ kuchni,
- rok budowy i typ użytkowania budynku,
- wskaźnik ryzyka `CRIME_INSPECTION_RISK_SCORE`,
- wskaźnik wieku budynku,
- wskaźnik zagęszczenia i jakości restauracji,
- wynik względny kuchni,
- dominujący typ przestępstwa w danym ZIP.

Wersja SQL została przygotowana po to, aby pokazać prowadzącemu, że ten sam proces można uruchomić nie tylko przez DataFrame API, ale też przez zapytania SQL tworzące tabele w bazie Databricks.

---

## 2. Środowisko uruchomieniowe

Notebook został przygotowany pod:

```text
Databricks Free Edition
Serverless compute
Unity Catalog
Python notebook
Spark SQL
Delta tables
```

Ważne ograniczenia Databricks Free Edition / serverless:

- nie używamy `spark.sparkContext`,
- nie używamy `df.rdd`,
- nie używamy `_jvm`,
- nie używamy `_jsc`,
- nie używamy `cache()`,
- nie używamy `persist()`,
- nie używamy bezpośredniego dostępu do HDFS.

Dane są przechowywane w Unity Catalog Volume, a wyniki są zapisywane jako tabele Delta w bazie Databricks.

---

## 3. Struktura danych w Databricks

W projekcie używane są:

```text
Catalog: workspace
Schema: default
Volume: project_files
```

Oznacza to, że ścieżka do volume ma postać:

```text
/Volumes/workspace/default/project_files
```

W tym katalogu powinny znajdować się pełne pliki CSV:

```text
/Volumes/workspace/default/project_files/DOHMH_latest.csv
/Volumes/workspace/default/project_files/PLUTO.csv
/Volumes/workspace/default/project_files/NYPD.csv
/Volumes/workspace/default/project_files/MODZCTA.csv
```

Dla danych testowych notebook zakłada podkatalog:

```text
/Volumes/workspace/default/project_files/test
```

i pliki:

```text
/Volumes/workspace/default/project_files/test/DOHMH_latest_test.csv
/Volumes/workspace/default/project_files/test/PLUTO_test.csv
/Volumes/workspace/default/project_files/test/NYPD_test.csv
/Volumes/workspace/default/project_files/test/MODZCTA_test.csv
```

---

## 4. Jak przenieść CSV do bazy danych Databricks

W Databricks dane z plików CSV można najpierw trzymać jako pliki w Volume, a następnie przenieść je do bazy jako tabele Delta.

W naszym notebooku robi to funkcja:

```python
load_csv_to_delta_table(path: str, table_name: str)
```

Przykładowo:

```python
load_csv_to_delta_table(DOHMH_CSV, DOHMH_TABLE)
```

wykonuje następujące operacje:

1. czyta CSV z volume przy użyciu `spark.read.csv`,
2. ustawia opcję `header=true`,
3. ustawia obsługę cudzysłowów i znaków escape,
4. normalizuje nazwy kolumn,
5. zapisuje wynik jako tabelę Delta przez `saveAsTable`,
6. liczy rekordy w utworzonej tabeli,
7. wypisuje status i czas ładowania.

Przykładowe tabele źródłowe dla pełnych danych:

```text
workspace.default.raw_dohmh_sql
workspace.default.raw_pluto_sql
workspace.default.raw_nypd_sql
workspace.default.raw_modzcta_sql
```

Dla danych testowych nazwy mają prefiks `test_`:

```text
workspace.default.test_raw_dohmh_sql
workspace.default.test_raw_pluto_sql
workspace.default.test_raw_nypd_sql
workspace.default.test_raw_modzcta_sql
```

### Dlaczego zapisujemy CSV do tabel Delta?

Plik CSV jest tylko plikiem tekstowym. Databricks potrafi go odczytać, ale praca na tabelach Delta jest wygodniejsza, ponieważ:

- tabele są widoczne w Catalog Explorer,
- można je odpytywać w SQL Editor,
- SQL może odwoływać się do nich po nazwie,
- wynikowe etapy pipeline mogą być zapisane jako tabele,
- łatwiej pokazać prowadzącemu „bazodanową” wersję przetwarzania.

---

## 5. Tryb testowy i pełny

Notebook ma zmienną:

```python
USE_TEST_DATA = True
```

Jeżeli `USE_TEST_DATA = True`, pipeline korzysta z małych plików testowych:

```text
/Volumes/workspace/default/project_files/test
```

i tworzy tabele z prefiksem `test_`.

Przykład:

```text
workspace.default.test_temp3_latest_sql
```

Jeżeli `USE_TEST_DATA = False`, pipeline korzysta z pełnych danych:

```text
/Volumes/workspace/default/project_files
```

i tworzy tabele bez prefiksu:

```text
workspace.default.temp3_latest_sql
```

Na początku warto użyć `USE_TEST_DATA = True`, bo wtedy można szybko sprawdzić, czy SQL działa poprawnie. Dopiero po udanym teście warto ustawić `USE_TEST_DATA = False` i uruchomić pipeline na pełnych danych.

---

## 6. Tabele wynikowe pipeline

Pipeline SQL tworzy cztery główne tabele wynikowe.

Dla danych testowych:

```text
workspace.default.test_temp1_latest_sql
workspace.default.test_nypd_zip_latest_sql
workspace.default.test_temp2_latest_sql
workspace.default.test_temp3_latest_sql
```

Dla pełnych danych:

```text
workspace.default.temp1_latest_sql
workspace.default.nypd_zip_latest_sql
workspace.default.temp2_latest_sql
workspace.default.temp3_latest_sql
```

Każda tabela odpowiada jednemu etapowi wcześniejszego pipeline MapReduce / Spark DataFrame.

---

## 7. Różnice względem podejścia DataFrame

### 7.1. Podejście DataFrame

W poprzedniej wersji pipeline logika była zapisana jako łańcuchy operacji DataFrame, np.:

```python
df.withColumn(...)
  .filter(...)
  .join(...)
  .groupBy(...)
  .agg(...)
```

Takie podejście jest typowe dla PySpark. Kod jest pisany w Pythonie, a Spark buduje plan wykonania na podstawie operacji DataFrame.

W podejściu DataFrame:

- przetwarzanie jest zapisane proceduralnie w Pythonie,
- kolejne etapy są funkcjami zwracającymi DataFrame,
- zapis wyników wykonywany jest przez `df.write`,
- wyniki były zapisywane głównie jako katalogi CSV w volume,
- trudniej pokazać całość w SQL Editor,
- użytkownik SQL nie widzi bezpośrednio logiki jako zapytań SQL.

### 7.2. Podejście SQL

W wersji SQL każdy etap buduje tabelę przez zapytanie:

```sql
CREATE OR REPLACE TABLE ... AS
WITH ...
SELECT ...
```

Czyli zamiast tworzyć DataFrame krok po kroku, budujemy zapytanie SQL, które opisuje cały etap.

W podejściu SQL:

- dane źródłowe są tabelami Delta,
- wyniki etapów są tabelami Delta,
- logika jest czytelna jako zapytania SQL,
- tabele są widoczne w Catalog Explorer,
- można je odpytywać z SQL Editor,
- łatwiej pokazać prowadzącemu efekt działania w stylu bazodanowym,
- każdy etap jest materializowany jako tabela.

### 7.3. Co pozostało w Pythonie?

Notebook nie jest w 100% czystym plikiem SQL, bo pewne elementy nadal są wygodniej obsłużyć w Pythonie:

1. automatyczne czyszczenie nazw kolumn,
2. dynamiczne budowanie SQL zależnie od dostępnych kolumn,
3. rejestracja funkcji `normalize_address_sql`,
4. rejestracja funkcji `find_zip_sql`,
5. parsowanie geometrii MODZCTA,
6. pomiar czasu etapów.

Sama logika etapów `TEMP1`, `NYPD_ZIP`, `TEMP2`, `TEMP3` jest jednak uruchamiana jako SQL przez:

```python
spark.sql(sql_text)
```

---

## 8. Ogólny przepływ notebooka

Notebook wykonuje następujące kroki:

```text
1. Import bibliotek i ustawienie timestampu
2. Konfiguracja catalog/schema/volume
3. Wybór trybu testowego lub pełnego
4. Wczytanie CSV do tabel Delta
5. Definicja funkcji pomocniczych do generowania SQL
6. Rejestracja funkcji SQL: normalize_address_sql
7. Rejestracja funkcji SQL: find_zip_sql
8. Zbudowanie SQL dla TEMP1
9. Zbudowanie SQL dla NYPD_ZIP
10. Zbudowanie SQL dla TEMP2
11. Zbudowanie SQL dla TEMP3
12. Uruchomienie etapów z pomiarem czasu
13. Weryfikacja tabeli końcowej
14. Opcjonalny eksport wyników do CSV
```

---

## 9. Szczegółowy opis elementów kodu

### 9.1. Import bibliotek

Na początku notebook importuje:

```python
from time import perf_counter
from datetime import datetime
import json
import math
import re

from pyspark.sql import functions as F
from pyspark.sql import types as T
```

Znaczenie:

- `perf_counter` — precyzyjny pomiar czasu etapów,
- `datetime` — timestamp uruchomienia i aktualny rok,
- `json` — parsowanie geometrii GeoJSON,
- `math` — obliczenia indeksu przestrzennego,
- `re` — wyrażenia regularne do czyszczenia tekstu,
- `F` — funkcje Spark używane przy imporcie i UDF,
- `T` — typy danych Spark, np. `StringType`.

Notebook wypisuje też wersję Spark:

```python
print("Spark version:", spark.version)
```

To jest bezpieczne w Databricks Free Edition, bo nie wymaga `sparkContext`.

---

### 9.2. Timestamp i rok bieżący

Kod:

```python
RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
CURRENT_YEAR = datetime.now().year
```

Znaczenie:

- `RUN_TS` może służyć do oznaczania uruchomienia,
- `CURRENT_YEAR` jest używany przy obliczaniu wieku budynku:

```text
BUILDING_AGE = CURRENT_YEAR - YEARBUILT
```

---

### 9.3. Konfiguracja katalogu i volume

Kod:

```python
CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "project_files"

BASE_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
```

Wynik:

```text
/Volumes/workspace/default/project_files
```

To główne miejsce przechowywania CSV w Databricks.

---

### 9.4. Funkcja `tbl(name)`

Kod:

```python
def tbl(name: str) -> str:
    return f"{DATABASE}.{TABLE_PREFIX}{name}"
```

Funkcja buduje pełną nazwę tabeli.

Dla danych testowych:

```python
tbl("temp3_latest_sql")
```

zwróci:

```text
workspace.default.test_temp3_latest_sql
```

Dla pełnych danych:

```text
workspace.default.temp3_latest_sql
```

Dzięki temu ten sam kod działa na testach i na pełnych danych.

---

### 9.5. Funkcja `canonical_name`

Kod:

```python
def canonical_name(name: str) -> str:
    ...
```

Funkcja zamienia nazwy kolumn na format bezpieczny dla SQL.

Przykłady:

```text
"CUISINE DESCRIPTION" -> "CUISINE_DESCRIPTION"
"INSPECTION DATE"    -> "INSPECTION_DATE"
"ZIP CODE"           -> "ZIP_CODE"
```

Dlaczego to ważne?

Oryginalne CSV mają kolumny ze spacjami i różnymi zapisami. Normalizacja pozwala pisać SQL w bardziej przewidywalny sposób.

---

### 9.6. Funkcja `load_csv_to_delta_table`

Kod:

```python
def load_csv_to_delta_table(path: str, table_name: str):
    ...
```

Ta funkcja przenosi pojedynczy plik CSV do tabeli Delta.

Wewnątrz:

1. odczytuje CSV:

```python
spark.read.option("header", "true").csv(path)
```

2. normalizuje nazwy kolumn:

```python
df = df.withColumnRenamed(c, canonical_name(c))
```

3. zapisuje tabelę Delta:

```python
df.write.format("delta").mode("overwrite").saveAsTable(table_name)
```

4. liczy rekordy:

```sql
SELECT COUNT(*) AS rows FROM table_name
```

5. wypisuje czas i status.

---

### 9.7. Funkcja `table_cols`

Kod:

```python
def table_cols(table_name: str):
    return {field.name.upper(): field.name for field in spark.table(table_name).schema.fields}
```

Funkcja zwraca słownik kolumn istniejących w tabeli.

Przykład wyniku:

```python
{
  "CAMIS": "CAMIS",
  "ZIPCODE": "ZIPCODE",
  "CUISINE_DESCRIPTION": "CUISINE_DESCRIPTION"
}
```

Funkcja jest potrzebna, ponieważ różne pliki mogą mieć lekko różne nazwy kolumn, np.:

```text
ZIPCODE
ZIP CODE
ZIP_CODE
```

---

### 9.8. Funkcja `qcol`

Kod:

```python
def qcol(cols: dict, alias: str, *names: str, default: str = "''") -> str:
    ...
```

Funkcja wybiera pierwszą istniejącą kolumnę z listy kandydatów.

Przykład:

```python
qcol(dcols, "d", "INSPECTION_DATE", "INSPECTION DATE")
```

może zwrócić:

```sql
d.`INSPECTION_DATE`
```

albo:

```sql
d.`INSPECTION DATE`
```

Jeżeli żadna kolumna nie istnieje, zwracana jest wartość domyślna:

```sql
''
```

Dzięki temu kod jest odporny na różnice nazw kolumn w CSV.

---

### 9.9. Funkcje generujące wyrażenia SQL

Notebook ma kilka funkcji, które zwracają fragmenty SQL.

#### `only_digits_sql(expr)`

Usuwa wszystko poza cyframi:

```sql
regexp_replace(..., '[^0-9]', '')
```

Używane dla:

- `CAMIS`,
- `ZIPCODE`,
- `BBL`,
- `YEARBUILT`.

#### `first_five_digits_sql(expr)`

Zwraca pierwsze 5 cyfr, czyli normalizuje ZIP:

```text
"10001-1234" -> "10001"
```

#### `safe_double_sql(expr)`

Bezpiecznie konwertuje tekst na `DOUBLE`.

Jeżeli wartość nie wygląda jak liczba, zwraca `NULL`.

#### `safe_int_sql(expr)`

Bezpiecznie konwertuje tekst na `INT`.

#### `norm_bbl_sql(expr)`

Normalizuje BBL, czyli identyfikator działki/budynku.

Niepoprawne albo puste BBL zamieniane są na pusty string.

#### `zscore_sql(value, mean, stdev)`

Zwraca wyrażenie SQL liczące standaryzację:

```text
(value - mean) / stdev
```

Jeżeli `stdev` jest zerowy albo `NULL`, wynik to `0.0`.

To zabezpiecza pipeline przed dzieleniem przez zero.

---

### 9.10. Funkcja `run_timed_sql`

Kod:

```python
def run_timed_sql(stage_name: str, sql_text: str, result_table: str):
    ...
```

Funkcja uruchamia jeden etap SQL.

Wykonuje:

```python
spark.sql(sql_text)
```

a potem:

```sql
SELECT COUNT(*) AS rows FROM result_table
```

Na końcu wypisuje:

- nazwę etapu,
- status `OK` albo `ERROR`,
- liczbę rekordów,
- czas trwania,
- ewentualny komunikat błędu.

Dzięki temu wynik nadaje się do zrzutu ekranu i opisu w sprawozdaniu.

---

## 10. Funkcje SQL / UDF

### 10.1. `normalize_address_sql`

Kod Pythona:

```python
spark.udf.register("normalize_address_sql", normalize_address_py, T.StringType())
```

Rejestruje funkcję, której można używać w SQL:

```sql
normalize_address_sql(...)
```

Funkcja:

- zamienia tekst na wielkie litery,
- usuwa kropki, przecinki i znaki specjalne,
- zwija wiele spacji do jednej,
- zamienia pełne nazwy ulic na skróty, np.:
  - `STREET` -> `ST`,
  - `AVENUE` -> `AVE`,
  - `ROAD` -> `RD`,
  - `BOULEVARD` -> `BLVD`,
  - `NORTH` -> `N`,
  - `SOUTH` -> `S`.

Jest używana przy łączeniu danych DOHMH z PLUTO po adresie.

---

### 10.2. `find_zip_sql`

Kod:

```python
spark.udf.register("find_zip_sql", find_zip_py, T.StringType())
```

Rejestruje funkcję SQL:

```sql
find_zip_sql(LATITUDE, LONGITUDE)
```

Funkcja przypisuje punkt NYPD do ZIP/MODZCTA.

Dlaczego potrzebujemy UDF?

Zwykły Spark SQL w Databricks Free Edition nie ma w tym notebooku gotowej funkcji typu `ST_Contains` działającej na geometrii MODZCTA, dlatego geometria została obsłużona własną funkcją.

---

## 11. Jak działa geometria w `NYPD_ZIP`

Etap `NYPD_ZIP` używa pliku `MODZCTA.csv`, który zawiera geometrie obszarów ZIP/MODZCTA.

Kod obsługuje dwa formaty geometrii:

- WKT:
  - `POLYGON`,
  - `MULTIPOLYGON`,
- GeoJSON:
  - `coordinates`.

### 11.1. Parsowanie geometrii

Funkcje:

```python
parse_wkt_geometry(...)
parse_geojson_geometry(...)
parse_any_geometry(...)
```

zamieniają tekst geometrii na listy punktów:

```text
[(lat1, lon1), (lat2, lon2), ...]
```

### 11.2. Bounding box

Dla każdego poligonu liczony jest prostokąt ograniczający:

```text
min_lat, max_lat, min_lon, max_lon
```

Dzięki temu przed wykonaniem dokładnego testu można szybko sprawdzić, czy punkt w ogóle znajduje się w pobliżu poligonu.

### 11.3. Indeks siatki

Funkcja:

```python
build_spatial_grid(...)
```

tworzy prosty indeks przestrzenny.

Zamiast sprawdzać każdy punkt NYPD ze wszystkimi poligonami MODZCTA, kod sprawdza tylko poligony z tej samej komórki siatki.

To znacząco ogranicza liczbę testów geometrycznych.

### 11.4. Point-in-polygon

Funkcja:

```python
point_in_ring(lat, lon, ring)
```

wykonuje dokładny test, czy punkt znajduje się wewnątrz poligonu.

Dopiero jeśli punkt:

1. trafia do odpowiedniej komórki siatki,
2. mieści się w bounding boxie,
3. przechodzi test point-in-polygon,

otrzymuje kod ZIP.

---

## 12. Etap `TEMP1_SQL`

Funkcja budująca SQL:

```python
build_temp1_sql()
```

tworzy zapytanie:

```sql
CREATE OR REPLACE TABLE ... AS
WITH ...
SELECT ...
```

### 12.1. Cel etapu

`TEMP1_SQL`:

- czyści DOHMH,
- usuwa rekordy z datą `1900-01-01`,
- wylicza statystyki po ZIP,
- wylicza statystyki po BORO,
- wylicza średnie po parze `BORO + CUISINE_DESCRIPTION`,
- łączy rekordy z PLUTO,
- tworzy tabelę pośrednią z danymi restauracji i budynków.

### 12.2. Główne CTE w `TEMP1`

#### `dohmh_base`

Przygotowuje kolumny:

- `INSPECTION_DATE_NORM`,
- `ZIPCODE`,
- `CAMIS`,
- `SCORE`,
- `CUISINE_DESCRIPTION`,
- `BUILDING`,
- `STREET`,
- `BORO`,
- `BBL`,
- `ADDRESS`.

#### `dohmh`

Filtruje dane:

- usuwa datę `1900`,
- wymaga ZIP,
- wymaga CAMIS,
- wymaga SCORE,
- wymaga BUILDING,
- wymaga STREET,
- wymaga BORO.

#### `zip_stats`

Liczy:

```text
NUMBER_PER_ZIP
AVG_SCORE_ZIP
```

#### `boro_stats`

Liczy:

```text
NUMBER_PER_BORO
```

#### `boro_cd_stats`

Liczy:

```text
AVG_SCORE_BORO_CD
```

czyli średni wynik inspekcji dla danej kombinacji dzielnicy i typu kuchni.

#### `pluto_base`

Przygotowuje PLUTO:

- normalizuje adres,
- normalizuje ZIP,
- normalizuje BBL,
- pobiera `LANDUSE`,
- pobiera `YEARBUILT`.

#### `pluto_bbl`

Tworzy klucz łączenia po BBL:

```text
B|BBL
```

#### `pluto_addr`

Tworzy klucz łączenia po ZIP i adresie:

```text
A|ZIPCODE|ADDRESS
```

#### `pluto_join`

Łączy oba sposoby dopasowania PLUTO.

#### `dohmh_join`

Dla DOHMH wybiera klucz join:

- jeśli jest BBL, używa `B|BBL`,
- jeśli nie ma BBL, używa `A|ZIPCODE|ADDRESS`.

### 12.3. Wynik `TEMP1`

Tabela zawiera m.in.:

```text
ZIPCODE
CAMIS
SCORE
CUISINE_DESCRIPTION
ADDRESS
BORO
NUMBER_PER_ZIP
NUMBER_PER_BORO
AVG_SCORE_ZIP
AVG_SCORE_BORO_CD
LANDUSE
YEARBUILT
```

---

## 13. Etap `NYPD_ZIP_SQL`

Funkcja budująca SQL:

```python
build_nypd_zip_sql()
```

### 13.1. Cel etapu

Oryginalny plik NYPD nie ma bezpośrednio kolumny ZIP. Etap przypisuje ZIP do zdarzenia na podstawie współrzędnych:

```text
LATITUDE
LONGITUDE
```

### 13.2. Główne CTE

#### `nypd_prepared`

Przygotowuje:

- `CMPLNT_NUM`,
- `OFNS_DESC`,
- `BORO_NM`,
- `LATITUDE`,
- `LONGITUDE`.

#### `filtered`

Usuwa rekordy:

- bez numeru skargi,
- bez typu zdarzenia,
- bez współrzędnych,
- ze współrzędnymi poza zakresem NYC.

Zakres:

```text
LATITUDE  od 40.0 do 41.2
LONGITUDE od -75.2 do -72.8
```

#### `matched`

Wywołuje:

```sql
find_zip_sql(LATITUDE, LONGITUDE)
```

i przypisuje `ZIPCODE`.

### 13.3. Wynik `NYPD_ZIP`

Tabela zawiera:

```text
CMPLNT_NUM
ZIPCODE
OFNS_DESC
BORO_NM
LATITUDE
LONGITUDE
```

---

## 14. Etap `TEMP2_SQL`

Funkcja budująca SQL:

```python
build_temp2_sql()
```

### 14.1. Cel etapu

`TEMP2_SQL` łączy dane restauracyjne z agregatami przestępczości.

### 14.2. Główne CTE

#### `temp1`

Ponownie normalizuje i typuje kolumny z `TEMP1`.

#### `zip_unique`

Tworzy unikalne rekordy ZIP potrzebne do statystyk.

#### `boro_unique`

Tworzy unikalne rekordy BORO potrzebne do statystyk.

#### `stdev_per_zip`

Liczy:

```text
STDEV_PER_ZIP
```

czyli odchylenie standardowe liczby restauracji per ZIP.

#### `stdev_per_boro`

Liczy:

```text
STDEV_PER_BORO
```

czyli odchylenie standardowe liczby restauracji per BORO.

#### `crime_base`

Przygotowuje dane z `NYPD_ZIP`:

- normalizuje ZIP,
- czyści numer skargi,
- czyści typ przestępstwa,
- usuwa duplikaty.

#### `crime_count`

Liczy:

```text
COUNT_CRIME_PER_ZIP
```

czyli liczbę unikalnych zdarzeń NYPD per ZIP.

#### `offense_counts`

Liczy liczbę typów przestępstw w każdym ZIP.

#### `dominant_ranked`

Nadaje ranking typom przestępstw w ZIP. Najczęstszy typ dostaje `rn = 1`. W przypadku remisu wygrywa typ alfabetycznie pierwszy.

#### `dominant`

Wybiera:

```text
DOMINANT_CRIME_TYPE
```

#### `crime_aligned`

Dopasowuje dane przestępczości do ZIP-ów występujących w restauracjach.

Jeżeli dla ZIP nie ma przestępstw, wpisuje:

```text
COUNT_CRIME_PER_ZIP = 0
DOMINANT_CRIME_TYPE = ''
```

#### `crime_stats`

Liczy:

```text
AVG_CRIME_PER_ZIP
STDEV_CRIME_PER_ZIP
```

#### `score_stats`

Liczy:

```text
MEAN_AVG_SCORE_ZIP
STDEV_AVG_SCORE_ZIP
```

### 14.3. Wynik `TEMP2`

Tabela zawiera dane z `TEMP1` rozszerzone m.in. o:

```text
STDEV_PER_ZIP
STDEV_PER_BORO
RESTAURANT_DENSITY_QUALITY_INDEX
BUILDING_AGE
AVG_CRIME_PER_ZIP
COUNT_CRIME_PER_ZIP
DOMINANT_CRIME_TYPE
CRIME_INSPECTION_RISK_SCORE
```

Na tym etapie `CRIME_INSPECTION_RISK_SCORE` jest wynikiem pośrednim. Finalne przeliczenie następuje w `TEMP3`.

---

## 15. Etap `TEMP3_SQL`

Funkcja budująca SQL:

```python
build_temp3_sql()
```

### 15.1. Cel etapu

`TEMP3_SQL` tworzy finalną tabelę wynikową.

### 15.2. Główne CTE

#### `temp2`

Przygotowuje typy danych z `TEMP2`.

#### `stdev_score_zip`

Liczy odchylenie standardowe `SCORE` w każdym ZIP:

```text
STDEV_SCORE_ZIP
```

#### `stdev_score_boro_cd`

Liczy odchylenie standardowe `SCORE` dla pary:

```text
BORO + CUISINE_DESCRIPTION
```

#### `stdev_crime`

Liczy odchylenie standardowe liczby przestępstw per ZIP.

### 15.3. Wskaźniki finalne

#### `CRIME_INSPECTION_RISK_SCORE`

Liczony jako suma dwóch komponentów:

```text
NORM_SCORE_ZIP + NORM_CRIME_ZIP
```

gdzie:

```text
NORM_SCORE_ZIP = (SCORE - AVG_SCORE_ZIP) / STDEV_SCORE_ZIP
NORM_CRIME_ZIP = (COUNT_CRIME_PER_ZIP - AVG_CRIME_PER_ZIP) / STDEV_CRIME_PER_ZIP
```

Jeżeli odchylenie standardowe jest zerowe albo puste, komponent ma wartość `0`.

#### `CUISINE_RELATIVE_SCORE`

Liczony jako:

```text
(SCORE - AVG_SCORE_BORO_CD) / STDEV_SCORE_BORO_CD
```

Wartość dodatnia oznacza wynik inspekcji gorszy od średniej dla danej kuchni w danej dzielnicy, bo w danych DOHMH większy `SCORE` oznacza więcej punktów naruszeń.

#### `BUILDING_AGE_SCORE`

W praktyce jest to `BUILDING_AGE`, czyli:

```text
CURRENT_YEAR - YEARBUILT
```

### 15.4. Finalny schemat

Tabela `TEMP3` zawiera:

```text
CAMIS
BORO
ZIPCODE
ADDRESS
CUISINE_DESCRIPTION
YEARBUILT
LANDUSE
CRIME_INSPECTION_RISK_SCORE
BUILDING_AGE_SCORE
RESTAURANT_DENSITY_QUALITY_INDEX
CUISINE_RELATIVE_SCORE
DOMINANT_CRIME_TYPE
```

---

## 16. Uruchamianie pipeline

Główna flaga:

```python
RUN_SQL_PIPELINE = True
```

Jeżeli jest ustawiona na `True`, notebook wykonuje po kolei:

```text
TEMP1_SQL
NYPD_ZIP_SQL
TEMP2_SQL
TEMP3_SQL
```

Każdy etap jest uruchamiany przez:

```python
run_timed_sql(...)
```

Na końcu drukowane jest podsumowanie:

```text
===== PODSUMOWANIE CZASÓW PIPELINE SQL =====
TEMP1_SQL: ...
NYPD_ZIP_SQL: ...
TEMP2_SQL: ...
TEMP3_SQL: ...
Łączny czas etapów SQL: ...
```

---

## 17. Weryfikacja wyników

Po wykonaniu pipeline można sprawdzić wynik końcowy w notebooku:

```python
display(spark.sql("""
SELECT *
FROM {TEMP3_TABLE}
LIMIT 20
"""))
```

oraz agregację:

```sql
SELECT
    BORO,
    COUNT(*) AS RECORDS,
    round(avg(CRIME_INSPECTION_RISK_SCORE), 4) AS AVG_RISK_SCORE,
    round(avg(CUISINE_RELATIVE_SCORE), 4) AS AVG_CUISINE_RELATIVE_SCORE,
    round(avg(RESTAURANT_DENSITY_QUALITY_INDEX), 4) AS AVG_RESTAURANT_DENSITY_QUALITY_INDEX
FROM TEMP3
GROUP BY BORO
ORDER BY BORO
```

To są dobre wyniki do screenów w sprawozdaniu, bo pokazują:

- że finalna tabela istnieje,
- ile ma rekordów,
- że można ją odpytywać SQL-em,
- że wyniki dają się agregować w bazie.

---

## 18. Zapytania do SQL Editor

Po uruchomieniu notebooka można wejść do SQL Editor i wykonać:

Dla danych testowych:

```sql
SELECT COUNT(*)
FROM workspace.default.test_temp3_latest_sql;
```

```sql
SELECT *
FROM workspace.default.test_temp3_latest_sql
LIMIT 20;
```

```sql
SELECT
    BORO,
    COUNT(*) AS RECORDS,
    ROUND(AVG(CRIME_INSPECTION_RISK_SCORE), 4) AS AVG_RISK_SCORE
FROM workspace.default.test_temp3_latest_sql
GROUP BY BORO
ORDER BY BORO;
```

Dla pełnych danych:

```sql
SELECT COUNT(*)
FROM workspace.default.temp3_latest_sql;
```

```sql
SELECT *
FROM workspace.default.temp3_latest_sql
LIMIT 20;
```

---

## 19. Opcjonalny eksport wyników do CSV

Domyślnie wyniki pozostają jako tabele Delta.

Jeżeli potrzebny jest eksport do CSV, można ustawić:

```python
EXPORT_RESULTS_TO_CSV = True
```

Wtedy notebook wykona:

```python
export_table_to_csv_dir(...)
```

i zapisze katalogi CSV w:

```text
/Volumes/workspace/default/project_files/results_sql_test
```

albo:

```text
/Volumes/workspace/default/project_files/results_sql
```

W Databricks zapis CSV przez Spark tworzy katalog z plikami `part-*`, a nie jeden pojedynczy plik CSV.

---

## 20. Co warto pokazać prowadzącemu

Dobre screeny do sprawozdania:

1. Catalog Explorer z tabelami `raw_*`.
2. Catalog Explorer z tabelami `temp*_latest_sql`.
3. Wynik `SELECT COUNT(*) FROM ...temp3_latest_sql`.
4. Wynik `SELECT * FROM ...temp3_latest_sql LIMIT 20`.
5. Agregacja po `BORO`.
6. Log notebooka z czasami `TEMP1_SQL`, `NYPD_ZIP_SQL`, `TEMP2_SQL`, `TEMP3_SQL`.
7. Fragment kodu `CREATE OR REPLACE TABLE ... AS WITH ... SELECT ...`.

---

## 21. Najczęstsze problemy i rozwiązania

### Problem: tabela nie istnieje

Komunikat może wyglądać jak:

```text
TABLE_OR_VIEW_NOT_FOUND
```

Rozwiązanie:

- sprawdzić, czy wykonano komórkę ładowania CSV do Delta,
- sprawdzić `USE_TEST_DATA`,
- sprawdzić prefiks `test_`,
- sprawdzić Catalog Explorer.

---

### Problem: plik CSV nie istnieje

Komunikat może wskazywać brak pliku w `/Volumes/...`.

Rozwiązanie:

- sprawdzić, czy pliki są w dobrym volume,
- sprawdzić, czy testowe pliki są w podkatalogu `test`,
- sprawdzić nazwy plików:
  - `DOHMH_latest.csv`,
  - `PLUTO.csv`,
  - `NYPD.csv`,
  - `MODZCTA.csv`.

---

### Problem: brak kolumny geometrii w MODZCTA

Notebook szuka kolumn takich jak:

```text
THE_GEOM
GEOMETRY
GEOM
SHAPE
WKT
NEW_GEOREFERENCED_COLUMN
GEOCODED_COLUMN
```

Jeżeli plik ma inną nazwę kolumny, trzeba dopisać ją do listy:

```python
geom_candidates = [...]
```

---

### Problem: `find_zip_sql` zwraca mało rekordów

Możliwe przyczyny:

- współrzędne NYPD są poza zakresem NYC,
- geometria MODZCTA nie została poprawnie rozpoznana,
- kolumna geometrii ma inny format,
- plik NYPD ma inne nazwy kolumn `LATITUDE` / `LONGITUDE`.

---

### Problem: kod działa wolno na pełnych danych

Najbardziej kosztowny może być etap `NYPD_ZIP_SQL`, ponieważ wykonuje UDF point-in-polygon dla dużej liczby rekordów NYPD.

Możliwe usprawnienia:

- najpierw testować na małym zbiorze,
- sprawdzić liczbę rekordów po filtrze współrzędnych,
- rozważyć użycie natywnych funkcji przestrzennych, jeśli są dostępne w danym środowisku Databricks,
- ograniczyć dane NYPD do potrzebnego zakresu dat, jeśli projekt na to pozwala.

---

## 22. Dlaczego wersja SQL jest przydatna w projekcie

Wersja SQL pokazuje, że pipeline można traktować jak proces bazodanowy:

- źródłowe CSV są przenoszone do tabel,
- każdy etap tworzy kolejną tabelę,
- wyniki są dostępne w Catalog Explorer,
- można je odpytywać w SQL Editor,
- można łatwo wykonać agregacje kontrolne,
- przetwarzanie jest bardziej zrozumiałe dla osób znających SQL.

W porównaniu do DataFrame podejście SQL jest bardziej deklaratywne: opisujemy, jaki wynik chcemy uzyskać, a Spark sam planuje wykonanie.

---

## 23. Skrócony przewodnik uruchomienia

### Krok 1 — wgraj pliki do volume

```text
/Volumes/workspace/default/project_files
```

### Krok 2 — dla testów ustaw

```python
USE_TEST_DATA = True
```

### Krok 3 — uruchom notebook

Najlepiej użyć:

```text
Run all
```

### Krok 4 — sprawdź tabele

W Catalog Explorer:

```text
workspace → default → Tables
```

### Krok 5 — sprawdź wynik końcowy

```sql
SELECT COUNT(*)
FROM workspace.default.test_temp3_latest_sql;
```

### Krok 6 — przełącz na pełne dane

```python
USE_TEST_DATA = False
```

i uruchom notebook ponownie.

---

## 24. Podsumowanie

Notebook `databricks_sql_pipeline_project.ipynb` przenosi projekt z podejścia Spark DataFrame na podejście oparte o SQL i tabele Delta w Databricks.

Najważniejsze cechy:

- wejściowe CSV są ładowane do tabel Delta,
- każdy etap pipeline jest reprezentowany jako tabela SQL,
- logika transformacji jest zapisana w zapytaniach `CREATE OR REPLACE TABLE ... AS SELECT`,
- etap geograficzny `NYPD_ZIP` wykorzystuje funkcję SQL `find_zip_sql`,
- wyniki są dostępne z poziomu Catalog Explorer i SQL Editor,
- notebook jest zgodny z ograniczeniami Databricks Free Edition / serverless compute,
- osoba znająca SQL może łatwiej prześledzić i zweryfikować działanie pipeline.

To podejście dobrze nadaje się do pokazania prowadzącemu, że dane zostały nie tylko przetworzone w Spark, ale również zapisane i analizowane w bazodanowym modelu pracy Databricks.

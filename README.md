# 📊 Data Ingestion Pipeline (Hadoop + HDFS)

## 📌 Opis projektu

Projekt realizuje proces **akwizycji danych (data ingestion)** z zewnętrznych źródeł Open Data oraz ich zapis do systemu plików **HDFS (Hadoop Distributed File System)**.

Pipeline został zaprojektowany tak, aby:

* pobierać dane z API NYC Open Data,
* obsługiwać zarówno dane statyczne, jak i dynamiczne,
* wykrywać przyrost danych (incremental load),
* automatycznie zapisywać dane do HDFS,
* prowadzić szczegółowe logi procesu.

---

## 📁 Źródła danych

Pipeline korzysta z trzech datasetów:

### 1. DOHMH (dynamiczny)

* Dane o inspekcjach restauracji NYC
* Źródła:

  * CSV (pełny dump)
  * JSON (do incremental load)

### 2. PLUTO (statyczny)

* Dane o nieruchomościach
* Pobierany tylko raz

### 3. NYPD (statyczny)

* Dane policyjne
* Pobierany tylko raz

---

## ⚙️ Architektura pipeline

Pipeline dzieli dane na dwa typy:

### 🔹 Dane statyczne

* Pobierane tylko jeśli plik nie istnieje
* Zapisywane lokalnie i do HDFS

### 🔹 Dane dynamiczne (DOHMH)

* Obsługiwane w trybie:

  * FULL LOAD (pierwsze uruchomienie)
  * INCREMENTAL LOAD (kolejne uruchomienia)

---

## 🔁 Mechanizm incremental load (DOHMH)

### Krok 1: Sprawdzenie pliku lokalnego

* Jeśli brak → FULL LOAD
* Jeśli istnieje → próbujemy znaleźć różnicę

---

### Krok 2: Odczyt ostatnich rekordów

* Pobierane jest ostatnie **100 wartości `camis`** z lokalnego pliku

---

### Krok 3: Pobieranie nowych danych

* Dane pobierane z endpointu JSON:

  * limity: `5000 → 10000 → 20000 → 50000`

---

### Krok 4: Szukanie matcha

* Szukamy pierwszego rekordu, którego `camis` występuje w ostatnich zapisanych danych
* Pozycja tego rekordu wyznacza punkt przecięcia

---

### Krok 5: Wyznaczenie nowych danych

* Wszystkie rekordy **przed match’em = nowe dane**

---

### Krok 6: Aktualizacja danych

* Nowe dane są dodawane na początek starego zbioru
* Tworzone są dwa pliki:

  * `DOHMH_<timestamp>.csv` → wersjonowany
  * `DOHMH_latest.csv` → aktualny snapshot

---

### Krok 7: Fallback

Jeśli nie znaleziono matcha:

* wykonywany jest FULL LOAD

---

## 💾 Zapis do HDFS

Każdy plik jest:

1. kopiowany do HDFS:

   ```
   /datasets/{dataset_name}/
   ```
2. nadpisywany jeśli istnieje
3. ustawiana jest replikacja:

   ```
   hdfs dfs -setrep -w 3
   ```

---

## 📊 Logowanie

Pipeline zapisuje logi w katalogu `logs/`:

Każdy log zawiera:

* timestamp
* poziom logu (INFO, WARN, ERROR, SUCCESS)
* opis operacji

### Przykładowe zdarzenia:

* rozpoczęcie pipeline
* pobieranie plików
* liczba nowych rekordów
* błędy HTTP
* fallback do FULL LOAD
* upload do HDFS

---

## 📥 Pobieranie danych

### Funkcja: `download_with_progress`

* pobiera pliki w trybie streaming
* pokazuje progress bar (`tqdm`)
* obsługuje duże pliki

---

## 🌐 Obsługa HTTP

### Funkcja: `safe_request`

* retry (3 próby)
* timeout
* logowanie błędów
* zabezpieczenie przed przerwaniem pipeline

---

## 🔍 Kluczowe funkcje

### `process_dohmh()`

* główna logika incremental load
* obsługa FULL / INCREMENTAL
* merge danych
* zapis do plików

---

### `find_match()`

* wyszukuje punkt przecięcia danych
* działa na podstawie pola `camis`

---

### `load_last_camis()`

* odczytuje końcowe rekordy z pliku
* przygotowuje dane do porównania

---

### `download_static_csv()`

* pobiera dane statyczne tylko raz
* zapobiega duplikacji

---

### `upload_to_hdfs()`

* wysyła pliki do HDFS
* ustawia replikację = 3

---

## ▶️ Uruchomienie

```bash
uv sync
uv run python main.py
```

---

## 📦 Wymagania

* Python 3.10+
* Hadoop + HDFS
* dostęp do komendy `hdfs dfs`

---

## 🧪 Zachowanie pipeline

| Scenariusz             | Zachowanie                  |
| ---------------------- | --------------------------- |
| Pierwsze uruchomienie  | FULL LOAD wszystkich danych |
| Kolejne uruchomienie   | Incremental dla DOHMH       |
| Brak matcha            | FULL LOAD                   |
| Błąd API               | retry                       |
| Brak pliku statycznego | pobranie                    |
| Plik istnieje          | pominięcie                  |

---

## 📈 Cechy rozwiązania

✔ obsługa dużych danych
✔ incremental ingestion
✔ odporność na błędy
✔ logowanie procesu
✔ integracja z Hadoop
✔ wersjonowanie danych
✔ progress bar

---

## ⚠️ Ograniczenia

* match oparty na `camis` może być wrażliwy na zmiany kolejności danych
* brak dedykowanego klucza czasowego w datasetach
* pełny reload w przypadku braku dopasowania

---

## 📌 Podsumowanie

Pipeline realizuje kompletny proces:

* pobrania danych,
* detekcji zmian,
* aktualizacji zbiorów,
* zapisu do HDFS,
* logowania i obsługi błędów.

Rozwiązanie spełnia wymagania zadania oraz odzwierciedla rzeczywiste podejście stosowane w systemach Big Data.

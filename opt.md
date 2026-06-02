Świetny wynik — spadek z około **10 minut do 7 sekund** jest duży, ale w tym przypadku logiczny, bo zoptymalizowaliśmy dokładnie najdroższy fragment: ręczne przypisywanie punktów NYPD do poligonów ZIP/MODZCTA. W poprzedniej wersji etap `NYPD_ZIP` działał przez Pythonowy UDF `find_zip_udf`, który dla każdego rekordu NYPD sprawdzał kolejne poligony MODZCTA. W zoptymalizowanej wersji dodałem indeks przestrzenny, filtrowanie przed geometrią, `mapPartitions`, `persist` i usunięcie ponownego przeliczania spatial join. 

## 1. Największa zmiana: indeks przestrzenny zamiast sprawdzania wszystkich ZIP-ów

W starej wersji logika była mniej więcej taka:

```text
dla każdego rekordu NYPD:
    dla każdego ZIP/MODZCTA:
        wykonaj point_in_polygon
```

Czyli jeżeli mamy przykładowo 1 000 000 rekordów NYPD i około 200 obszarów ZIP/MODZCTA, to w skrajnym przypadku robimy nawet:

```text
1 000 000 × 200 = 200 000 000 prób dopasowania
```

Do tego każda próba `point_in_polygon` przechodzi po punktach wielokąta, więc to nie jest tania operacja.

W nowej wersji dodałem siatkę przestrzenną:

```python
def build_spatial_grid(areas, grid_size=0.01):
```

Każdy poligon MODZCTA dostaje swój bounding box:

```python
min_lat, max_lat, min_lon, max_lon
```

a potem jest przypisany tylko do tych komórek siatki, które przecina. Dla punktu NYPD wyliczamy tylko jego komórkę:

```python
grid_key(lat, lon, grid_size)
```

i sprawdzamy tylko poligony z tej komórki.

Czyli po optymalizacji schemat jest taki:

```text
dla każdego rekordu NYPD:
    znajdź komórkę siatki
    pobierz kilka kandydatów ZIP/MODZCTA
    dopiero dla nich wykonaj point_in_polygon
```

To zmniejsza liczbę pełnych testów geometrycznych z setek na rekord do zwykle kilku lub jednego.

## 2. Bounding box odcina większość przypadków przed geometrią

Nawet po pobraniu kandydatów z siatki dodałem jeszcze szybki test:

```python
if lat_value < min_lat or lat_value > max_lat or lon_value < min_lon or lon_value > max_lon:
    continue
```

To jest bardzo tanie porównanie liczbowe. Dopiero gdy punkt przejdzie przez bounding box, uruchamia się pełne:

```python
point_in_ring(lat_value, lon_value, ring)
```

Czyli kosztowny algorytm ray casting wykonuje się tylko wtedy, kiedy punkt faktycznie może leżeć w danym poligonie.

## 3. Zmieniliśmy Python UDF na `mapPartitions`

W starej wersji było:

```python
.withColumn("ZIPCODE", find_zip_udf("LATITUDE", "LONGITUDE"))
```

To oznaczało klasyczny Python UDF w Spark SQL. Taki UDF ma spory narzut, bo Spark działa głównie w JVM, a dla UDF musi przekazywać dane do procesu Pythona i odbierać wynik.

W nowej wersji jest:

```python
nypd_prepared.rdd.mapPartitions(assign_zip_partition)
```

To jest korzystniejsze, bo:

```text
- funkcja działa na całej partycji, a nie jako osobne wywołanie SQL UDF dla każdego wiersza,
- broadcastowany indeks przestrzenny jest pobierany raz na partycję,
- mniej jest narzutu komunikacji Spark SQL ↔ Python UDF,
- łatwiej kontrolować logikę dopasowania punktu do ZIP.
```

To nie zawsze jest szybsze od natywnych funkcji Spark SQL, ale w tym konkretnym przypadku jest szybsze, bo operacja geometryczna i tak musi być wykonana w Pythonie.

## 4. Filtrowanie danych jest teraz przed geometrią

Dodałem etap przygotowania NYPD przed spatial joinem. Najpierw Spark natywnie odrzuca rekordy, które i tak nie mogłyby dostać ZIP-a:

```text
- brak CMPLNT_NUM,
- brak OFNS_DESC,
- brak LATITUDE,
- brak LONGITUDE,
- współrzędne spoza sensownego zakresu NYC.
```

To jest ważne, bo każdy odrzucony wcześniej rekord to rekord, dla którego nie trzeba wykonywać geometrii.

Stara wersja część tych kontroli robiła dopiero wewnątrz UDF-a. Nowa wersja przesuwa możliwie dużo pracy do natywnych operacji Spark, zanim Python zacznie liczyć poligony.

## 5. Usunęliśmy podwójne przeliczanie tego samego etapu

To był drugi bardzo ważny powód przyspieszenia.

W Spark DataFrame jest leniwy. To znaczy, że samo utworzenie:

```python
nypd_zip = ...
```

nie liczy danych. Liczenie zaczyna się dopiero przy akcji, np.:

```python
write
count
show
collect
```

W starej wersji działo się to mniej więcej tak:

```text
1. write_single_csv(nypd_zip)  → liczy spatial join i zapisuje wynik
2. df.count() w run_timed_stage → liczy spatial join drugi raz
```

Czyli najdroższy etap mógł odpalać się dwa razy.

W nowej wersji zrobiłem:

```python
nypd_zip = spark.createDataFrame(...).persist(StorageLevel.MEMORY_AND_DISK)

rows_before_write = nypd_zip.count()
write_single_csv(nypd_zip, ...)
nypd_zip.unpersist()
return read_csv(latest_file)
```

Efekt:

```text
- count materializuje wynik i zapisuje go w cache/dysku,
- write korzysta z tego samego policzonego wyniku,
- po zapisie zwracamy DataFrame z gotowego pliku latest,
- końcowy count w timerze nie uruchamia już spatial join od nowa.
```

To samo mogło dać bardzo duży zysk, bo wcześniej część tych 10 minut była po prostu ponownym wykonaniem tej samej geometrii.

## 6. Dlaczego przyspieszenie jest aż tak duże

Bo zoptymalizowaliśmy algorytmicznie najdroższy fragment.

Stary koszt był w przybliżeniu:

```text
liczba rekordów NYPD × liczba poligonów ZIP × liczba punktów w poligonie
```

Nowy koszt jest bliższy:

```text
liczba rekordów NYPD × liczba kandydatów z komórki siatki × liczba punktów w poligonie
```

Jeżeli wcześniej punkt sprawdzał np. 180 poligonów, a teraz sprawdza 1–5 kandydatów, to sama liczba prób geometrycznych mogła spaść kilkadziesiąt razy. Do tego doszło usunięcie drugiego przeliczenia i mniejszy narzut Python UDF. Dlatego spadek z 10 minut do kilku sekund jest możliwy.

## 7. Czy wynik nadal jest poprawny?

Tak, bo nie zmieniliśmy kryterium przypisania ZIP-a.

Nadal finalna decyzja jest podejmowana przez:

```python
point_in_ring(lat_value, lon_value, ring)
```

Czyli punkt musi faktycznie leżeć w poligonie MODZCTA.

Siatka i bounding box nie zastępują geometrii. One tylko ograniczają listę kandydatów. To znaczy:

```text
- bounding box mówi: ten poligon w ogóle może pasować,
- siatka mówi: sprawdź tylko poligony blisko punktu,
- point_in_ring mówi: ten punkt faktycznie leży w tym poligonie.
```

Dzięki temu zachowujemy poprawność, ale unikamy bezsensownego sprawdzania poligonów z innych części miasta.

## 8. Jak to opisać w raporcie

Możecie napisać:

```text
Pierwsza implementacja etapu NYPD_ZIP wykonywała przypisanie ZIP przez Python UDF, który dla każdego punktu NYPD sprawdzał przynależność do kolejnych poligonów MODZCTA. Była to operacja kosztowna, ponieważ złożoność była proporcjonalna do liczby rekordów NYPD pomnożonej przez liczbę poligonów.

W zoptymalizowanej wersji zbudowano prosty indeks przestrzenny w postaci siatki lat/lon oraz bounding boxów dla poligonów. Dzięki temu dla każdego punktu sprawdzano tylko kilka kandydackich poligonów znajdujących się w tej samej komórce siatki. Pełny test point-in-polygon wykonywano dopiero po przejściu szybkiego testu bounding box.

Dodatkowo zastąpiono Spark SQL Python UDF przez przetwarzanie partycji RDD mapPartitions, przeniesiono filtrowanie błędnych rekordów przed etap geometrii oraz zastosowano persist MEMORY_AND_DISK, aby uniknąć ponownego przeliczania spatial join podczas zapisu i liczenia rekordów.

Po optymalizacji czas etapu NYPD_ZIP spadł z około 10 minut do około 7 sekund, przy zachowaniu tej samej logiki przypisania ZIP/MODZCTA.
```

Najkrócej: **wcześniej każdy punkt pytał wszystkie ZIP-y “czy jestem u ciebie?”, a teraz pyta tylko kilka najbliższych kandydatów.**

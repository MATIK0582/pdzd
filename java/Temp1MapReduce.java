import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
















public class Temp1MapReduce extends Configured implements Tool {

    private static final String US = "\u001F"; 
    private static final String HEADER = "ZIPCODE,CAMIS,SCORE,CUISINE_DESCRIPTION,ADDRESS,BORO,NUMBER_PER_ZIP,NUMBER_PER_BORO,AVG_SCORE_ZIP,AVG_SCORE_BORO_CD,LANDUSE,YEARBUILT";

    // Punkt wejścia programu uruchamianego przez yarn jar.
    // Wejście: argumenty CLI przekazane do klasy MapReduce.
    // Wyjście: kod zakończenia procesu zwrócony przez ToolRunner.
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Temp1MapReduce(), args);
        System.exit(exitCode);
    }

    // Steruje pełnym przebiegiem etapu MapReduce: odczytuje argumenty, konfiguruje zadania Hadoop, uruchamia je w kolejności oraz zapisuje pliki wynikowe.
    // Wejście: ścieżki HDFS przekazane w argumentach lub wartości domyślne zapisane w kodzie.
    // Wyjście: plik CSV z timestampem, plik _latest.csv oraz kod statusu 0/1/2/... zależny od powodzenia poszczególnych jobów.
    @Override
    public int run(String[] args) throws Exception {
        String dohmhInput = args.length > 0 ? args[0] : "/datasets/dohmh/DOHMH_latest.csv";
        String plutoInput = args.length > 1 ? args[1] : "/datasets/static/PLUTO.csv";
        String resultsDir = args.length > 2 ? args[2] : "/datasets/results";

        Configuration baseConf = getConf();
        FileSystem fs = FileSystem.get(baseConf);
        fs.mkdirs(new Path(resultsDir));

        String ts = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        Path workDir = new Path(resultsDir + "/_temp1_work_" + ts);
        Path cleanOut = new Path(workDir, "01_clean_dohmh");
        Path statsOut = new Path(workDir, "02_stats");
        Path plutoOut = new Path(workDir, "03_pluto_by_address");
        Path finalOut = new Path(workDir, "04_final_temp1_dir");

        Path versionedFile = new Path(resultsDir + "/temp1_" + ts + ".csv");
        Path latestFile = new Path(resultsDir + "/temp1_latest.csv");

        if (fs.exists(workDir)) {
            fs.delete(workDir, true);
        }

        Map<String, Integer> dohmhHeader = readHeaderIndex(baseConf, new Path(dohmhInput));
        Map<String, Integer> plutoHeader = readHeaderIndex(baseConf, new Path(plutoInput));

        Configuration cleanConf = new Configuration(baseConf);
        setDohmhColumns(cleanConf, dohmhHeader);
        Job cleanJob = Job.getInstance(cleanConf, "TEMP1 step 1 - clean DOHMH");
        cleanJob.setJarByClass(Temp1MapReduce.class);
        cleanJob.setMapperClass(DohmhCleanMapper.class);
        cleanJob.setNumReduceTasks(0);
        cleanJob.setMapOutputKeyClass(NullWritable.class);
        cleanJob.setMapOutputValueClass(Text.class);
        cleanJob.setOutputKeyClass(NullWritable.class);
        cleanJob.setOutputValueClass(Text.class);
        cleanJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(cleanJob, new Path(dohmhInput));
        FileOutputFormat.setOutputPath(cleanJob, cleanOut);
        if (!cleanJob.waitForCompletion(true)) {
            return 1;
        }

        Configuration statsConf = new Configuration(baseConf);
        Job statsJob = Job.getInstance(statsConf, "TEMP1 step 2 - aggregate DOHMH stats");
        statsJob.setJarByClass(Temp1MapReduce.class);
        statsJob.setMapperClass(StatsMapper.class);
        statsJob.setReducerClass(StatsReducer.class);
        statsJob.setMapOutputKeyClass(Text.class);
        statsJob.setMapOutputValueClass(Text.class);
        statsJob.setOutputKeyClass(Text.class);
        statsJob.setOutputValueClass(Text.class);
        statsJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(statsJob, cleanOut);
        FileOutputFormat.setOutputPath(statsJob, statsOut);
        if (!statsJob.waitForCompletion(true)) {
            return 2;
        }

        Configuration plutoConf = new Configuration(baseConf);
        setPlutoColumns(plutoConf, plutoHeader);
        Job plutoJob = Job.getInstance(plutoConf, "TEMP1 step 3 - aggregate PLUTO by address");
        plutoJob.setJarByClass(Temp1MapReduce.class);
        plutoJob.setMapperClass(PlutoMapper.class);
        plutoJob.setReducerClass(PlutoReducer.class);
        plutoJob.setMapOutputKeyClass(Text.class);
        plutoJob.setMapOutputValueClass(Text.class);
        plutoJob.setOutputKeyClass(Text.class);
        plutoJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(plutoJob, new Path(plutoInput));
        FileOutputFormat.setOutputPath(plutoJob, plutoOut);
        if (!plutoJob.waitForCompletion(true)) {
            return 3;
        }

        Configuration finalConf = new Configuration(baseConf);
        finalConf.set("temp1.stats.path", statsOut.toString());
        Job finalJob = Job.getInstance(finalConf, "TEMP1 step 4 - join DOHMH stats with PLUTO");
        finalJob.setJarByClass(Temp1MapReduce.class);
        MultipleInputs.addInputPath(finalJob, cleanOut, TextInputFormat.class, FinalDohmhMapper.class);
        MultipleInputs.addInputPath(finalJob, plutoOut, TextInputFormat.class, FinalPlutoMapper.class);
        finalJob.setReducerClass(FinalReducer.class);
        finalJob.setMapOutputKeyClass(Text.class);
        finalJob.setMapOutputValueClass(Text.class);
        finalJob.setOutputKeyClass(NullWritable.class);
        finalJob.setOutputValueClass(Text.class);
        finalJob.setOutputFormatClass(TextOutputFormat.class);
        finalJob.setNumReduceTasks(1); 
        FileOutputFormat.setOutputPath(finalJob, finalOut);
        if (!finalJob.waitForCompletion(true)) {
            return 4;
        }

        Path partFile = findFirstPartFile(fs, finalOut);
        if (partFile == null) {
            throw new IOException("No part file found in " + finalOut);
        }

        if (fs.exists(versionedFile)) {
            fs.delete(versionedFile, false);
        }
        if (fs.exists(latestFile)) {
            fs.delete(latestFile, false);
        }

        copyHdfsFile(fs, partFile, versionedFile);
        copyHdfsFile(fs, partFile, latestFile);
        fs.setReplication(versionedFile, (short) 3);
        fs.setReplication(latestFile, (short) 3);

        
        fs.delete(workDir, true);

        System.out.println("TEMP1 versioned output: " + versionedFile);
        System.out.println("TEMP1 latest output:    " + latestFile);
        System.out.println("Requested HDFS replication factor: 3");
        return 0;
    }

    
    public static class DohmhCleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private int idxZip;
        private int idxCamis;
        private int idxScore;
        private int idxCuisine;
        private int idxBuilding;
        private int idxStreet;
        private int idxBoro;
        private int idxInspectionDate;
        private int idxBbl;
        private int maxIdx;

        // Pobiera z konfiguracji indeksy kolumn DOHMH wyznaczone wcześniej na podstawie nagłówka CSV.
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            idxZip = conf.getInt("dohmh.idx.zipcode", 5);
            idxCamis = conf.getInt("dohmh.idx.camis", 0);
            idxScore = conf.getInt("dohmh.idx.score", 13);
            idxCuisine = conf.getInt("dohmh.idx.cuisine", 7);
            idxBuilding = conf.getInt("dohmh.idx.building", 3);
            idxStreet = conf.getInt("dohmh.idx.street", 4);
            idxBoro = conf.getInt("dohmh.idx.boro", 2);
            idxInspectionDate = conf.getInt("dohmh.idx.inspection_date", 8);
            idxBbl = conf.getInt("dohmh.idx.bbl", 24);
            maxIdx = max(idxZip, idxCamis, idxScore, idxCuisine, idxBuilding, idxStreet, idxBoro, idxInspectionDate, idxBbl);
        }

        // Czyści pojedynczy rekord DOHMH i tworzy wewnętrzny rekord TEMP1 używany w kolejnych jobach.
        // Wejście: surowy wiersz DOHMH_latest.csv.
        // Wyjście: wiersz z polami ZIPCODE, CAMIS, SCORE, CUISINE, ADDRESS, BORO, normalizedAddress, BBL oddzielonymi znakiem US; rekordy błędne są pomijane.
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) {
                return;
            }

            List<String> f = parseCsvLine(line);
            if (isCsvHeader(f, "CAMIS")) {
                return;
            }
            if (f.size() <= maxIdx) {
                return;
            }

            String inspectionDate = clean(get(f, idxInspectionDate));
            if (isInspectionDate1900(inspectionDate)) {
                return;
            }

            String zip = onlyDigits(clean(get(f, idxZip)));
            String camis = onlyDigits(clean(get(f, idxCamis)));
            String scoreRaw = clean(get(f, idxScore));
            String cuisine = clean(get(f, idxCuisine));
            String building = clean(get(f, idxBuilding));
            String street = clean(get(f, idxStreet));
            String boro = clean(get(f, idxBoro));
            String bbl = normalizeBbl(clean(get(f, idxBbl)));

            
            if (isBlank(zip) || isBlank(camis) || isBlank(scoreRaw) || isBlank(building) || isBlank(street) || isBlank(boro)) {
                return;
            }

            Double score = parseDouble(scoreRaw);
            if (score == null) {
                return;
            }

            if (isBlank(cuisine)) {
                cuisine = "UNKNOWN";
            }

            String boroUpper = upper(boro);
            String addressDisplay = normalizeAddress(building + " " + street);
            String normalizedAddress = addressDisplay;
            String scoreOut = formatScore(score);

            String[] out = new String[] {
                    zip,
                    camis,
                    scoreOut,
                    upper(cuisine),
                    addressDisplay,
                    boroUpper,
                    normalizedAddress,
                    bbl
            };
            context.write(NullWritable.get(), new Text(joinUS(out)));
        }
    }

    
    public static class StatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        // Z jednego oczyszczonego wiersza DOHMH tworzy rekordy pomocnicze do agregacji po ZIP, BORO oraz BORO+CUISINE.
        // Wejście: wewnętrzny wiersz z DohmhCleanMapper.
        // Wyjście: pary klucz-wartość ZIP|..., BORO|..., BOROCD|... dla reducerów statystycznych.
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] f = splitUS(value.toString());
            if (f.length < 7) {
                return;
            }
            String zip = f[0];
            String camis = f[1];
            String score = f[2];
            String cuisine = f[3];
            String boro = f[5];

            outKey.set("ZIP|" + zip);
            outValue.set(camis + "|" + score);
            context.write(outKey, outValue);

            outKey.set("BORO|" + boro);
            outValue.set(camis);
            context.write(outKey, outValue);

            outKey.set("BOROCD|" + boro + "|" + cuisine);
            outValue.set(score);
            context.write(outKey, outValue);
        }
    }

    
    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {
        // Liczy statystyki TEMP1: COUNT(DISTINCT CAMIS) po ZIP/BORO oraz średnie SCORE po ZIP i BORO+CUISINE.
        // Wejście: grupa wartości dla jednego klucza ZIP, BORO albo BOROCD.
        // Wyjście: pojedynczy rekord statystyczny zapisywany do katalogu 02_stats.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String k = key.toString();
            if (k.startsWith("ZIP|")) {
                Set<String> distinctCamis = new HashSet<String>();
                double scoreSum = 0.0;
                long scoreCount = 0L;
                for (Text v : values) {
                    String[] p = v.toString().split("\\|", -1);
                    if (p.length >= 2) {
                        distinctCamis.add(p[0]);
                        Double score = parseDouble(p[1]);
                        if (score != null) {
                            scoreSum += score.doubleValue();
                            scoreCount++;
                        }
                    }
                }
                String avg = scoreCount == 0 ? "" : formatDouble(scoreSum / scoreCount);
                context.write(key, new Text(distinctCamis.size() + "|" + avg));
            } else if (k.startsWith("BORO|")) {
                Set<String> distinctCamis = new HashSet<String>();
                for (Text v : values) {
                    String camis = v.toString();
                    if (!isBlank(camis)) {
                        distinctCamis.add(camis);
                    }
                }
                context.write(key, new Text(String.valueOf(distinctCamis.size())));
            } else if (k.startsWith("BOROCD|")) {
                double scoreSum = 0.0;
                long scoreCount = 0L;
                for (Text v : values) {
                    Double score = parseDouble(v.toString());
                    if (score != null) {
                        scoreSum += score.doubleValue();
                        scoreCount++;
                    }
                }
                String avg = scoreCount == 0 ? "" : formatDouble(scoreSum / scoreCount);
                context.write(key, new Text(avg));
            }
        }
    }

    
    public static class PlutoMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int idxAddress;
        private int idxPostcode;
        private int idxBbl;
        private int idxLanduse;
        private int idxYearbuilt;
        private int maxIdx;
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        // Pobiera z konfiguracji indeksy kolumn PLUTO: ADDRESS, POSTCODE, BBL, LANDUSE i YEARBUILT.
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            idxAddress = conf.getInt("pluto.idx.address", 16);
            idxPostcode = conf.getInt("pluto.idx.postcode", 8);
            idxBbl = conf.getInt("pluto.idx.bbl", 68);
            idxLanduse = conf.getInt("pluto.idx.landuse", 33);
            idxYearbuilt = conf.getInt("pluto.idx.yearbuilt", 75);
            maxIdx = max(idxAddress, idxPostcode, idxBbl, idxLanduse, idxYearbuilt);
        }

        // Normalizuje adresy PLUTO i emituje kandydatów do późniejszego joinu z DOHMH.
        // Wejście: surowy wiersz PLUTO.csv.
        // Wyjście: rekord PLUTO pod kluczem BBL oraz/lub ZIP+ADDRESS, zawierający LANDUSE i YEARBUILT.
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) {
                return;
            }
            List<String> f = parseCsvLine(line);
            if (isCsvHeader(f, "ADDRESS")) {
                return;
            }
            if (f.size() <= maxIdx) {
                return;
            }

            String address = clean(get(f, idxAddress));
            if (isBlank(address)) {
                return;
            }
            String normalizedAddress = normalizeAddress(address);
            if (isBlank(normalizedAddress)) {
                return;
            }

            String zip = firstFiveDigits(clean(get(f, idxPostcode)));
            String bbl = normalizeBbl(clean(get(f, idxBbl)));
            String landuse = clean(get(f, idxLanduse));
            String yearbuilt = onlyDigits(clean(get(f, idxYearbuilt)));
            outValue.set(key.get() + "|" + safePipe(landuse) + "|" + safePipe(yearbuilt));

            
            
            if (!isBlank(bbl)) {
                outKey.set("B|" + bbl);
                context.write(outKey, outValue);
            }
            if (!isBlank(zip)) {
                outKey.set("A|" + zip + "|" + normalizedAddress);
                context.write(outKey, outValue);
            }
        }
    }

    
    public static class PlutoReducer extends Reducer<Text, Text, Text, Text> {
        // Agreguje wiele rekordów PLUTO dla tego samego klucza joinu do jednej reprezentacji budynku/działki.
        // Wejście: kandydaci PLUTO dla jednego BBL albo ZIP+ADDRESS.
        // Wyjście: pierwszy niepusty LANDUSE według pozycji w pliku i najnowszy YEARBUILT.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String firstLanduse = "";
            long firstLanduseOffset = Long.MAX_VALUE;
            int newestYear = 0;

            for (Text v : values) {
                String[] p = v.toString().split("\\|", -1);
                if (p.length < 3) {
                    continue;
                }
                long offset = parseLongOrMax(p[0]);
                String landuse = p[1];
                Integer year = parseInt(p[2]);

                if (!isBlank(landuse) && offset < firstLanduseOffset) {
                    firstLanduse = landuse;
                    firstLanduseOffset = offset;
                }
                if (year != null && year.intValue() > newestYear) {
                    newestYear = year.intValue();
                }
            }

            String yearOut = newestYear > 0 ? String.valueOf(newestYear) : "";
            context.write(key, new Text(firstLanduse + "|" + yearOut));
        }
    }

    
    public static class FinalDohmhMapper extends Mapper<LongWritable, Text, Text, Text> {
        // Mapuje oczyszczone wiersze DOHMH na klucz joinu z PLUTO: najpierw BBL, a jeśli go brak, ZIP+ADDRESS.
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] f = splitUS(value.toString());
            if (f.length < 8) {
                return;
            }
            String zip = f[0];
            String normalizedAddress = f[6];
            String bbl = f[7];
            String joinKey = makeJoinKey(zip, bbl, normalizedAddress);
            if (isBlank(joinKey)) {
                return;
            }
            context.write(new Text(joinKey), new Text("D" + US + value.toString()));
        }
    }

    
    public static class FinalPlutoMapper extends Mapper<LongWritable, Text, Text, Text> {
        // Przepisuje zagregowane rekordy PLUTO do formatu wejściowego finalnego reduce-side joinu.
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int tab = line.indexOf('\t');
            if (tab < 0) {
                return;
            }
            String normalizedAddress = line.substring(0, tab);
            String payload = line.substring(tab + 1);
            String[] p = payload.split("\\|", -1);
            String landuse = p.length > 0 ? p[0] : "";
            String yearbuilt = p.length > 1 ? p[1] : "";
            context.write(new Text(normalizedAddress), new Text("P" + US + landuse + US + yearbuilt));
        }
    }

    
    public static class FinalReducer extends Reducer<Text, Text, NullWritable, Text> {
        private final Map<String, ZipStat> zipStats = new HashMap<String, ZipStat>();
        private final Map<String, String> boroCounts = new HashMap<String, String>();
        private final Map<String, String> boroCuisineAvg = new HashMap<String, String>();

        // Ładuje wcześniej wyliczone statystyki TEMP1 do map w pamięci oraz zapisuje nagłówek pliku wynikowego.
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            loadStats(context.getConfiguration());
            context.write(NullWritable.get(), new Text(HEADER));
        }

        // Łączy rekordy DOHMH z dopasowanymi danymi PLUTO oraz dopisuje statystyki po ZIP/BORO/BORO+CUISINE.
        // Wejście: dla jednego klucza joinu lista rekordów typu D (DOHMH) i P (PLUTO).
        // Wyjście: finalne wiersze temp1 CSV w relacji 1:1 względem oczyszczonych rekordów DOHMH.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String landuse = "";
            String yearbuilt = "";
            List<String[]> rows = new ArrayList<String[]>();

            for (Text v : values) {
                String[] p = splitUS(v.toString());
                if (p.length == 0) {
                    continue;
                }
                if ("P".equals(p[0])) {
                    if (p.length > 1 && !isBlank(p[1]) && isBlank(landuse)) {
                        landuse = p[1];
                    }
                    if (p.length > 2 && !isBlank(p[2]) && isBlank(yearbuilt)) {
                        yearbuilt = p[2];
                    }
                } else if ("D".equals(p[0])) {
                    
                    if (p.length >= 8) {
                        String[] row = new String[] { p[1], p[2], p[3], p[4], p[5], p[6], p[7] };
                        rows.add(row);
                    }
                }
            }

            for (String[] r : rows) {
                String zip = r[0];
                String camis = r[1];
                String score = r[2];
                String cuisine = r[3];
                String address = r[4];
                String boro = r[5];

                ZipStat zs = zipStats.get(zip);
                String numberPerZip = zs == null ? "" : zs.count;
                String avgScoreZip = zs == null ? "" : zs.avgScore;
                String numberPerBoro = getOrEmpty(boroCounts, boro);
                String avgScoreBoroCd = getOrEmpty(boroCuisineAvg, boro + "|" + cuisine);

                String[] csv = new String[] {
                        zip,
                        camis,
                        score,
                        cuisine,
                        address,
                        boro,
                        numberPerZip,
                        numberPerBoro,
                        avgScoreZip,
                        avgScoreBoroCd,
                        landuse,
                        yearbuilt
                };
                context.write(NullWritable.get(), new Text(toCsv(csv)));
            }
        }

        // Wczytuje pliki part- ze statystykami TEMP1 i buduje mapy zipStats, boroCounts i boroCuisineAvg.
        private void loadStats(Configuration conf) throws IOException {
            String statsPath = conf.get("temp1.stats.path");
            if (statsPath == null) {
                throw new IOException("Missing temp1.stats.path configuration");
            }
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] files = fs.listStatus(new Path(statsPath));
            for (FileStatus st : files) {
                if (!st.isFile() || !st.getPath().getName().startsWith("part-")) {
                    continue;
                }
                FSDataInputStream in = fs.open(st.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
                try {
                    String line;
                    while ((line = br.readLine()) != null) {
                        int tab = line.indexOf('\t');
                        if (tab < 0) {
                            continue;
                        }
                        String key = line.substring(0, tab);
                        String val = line.substring(tab + 1);
                        if (key.startsWith("ZIP|")) {
                            String zip = key.substring("ZIP|".length());
                            String[] p = val.split("\\|", -1);
                            String count = p.length > 0 ? p[0] : "";
                            String avg = p.length > 1 ? p[1] : "";
                            zipStats.put(zip, new ZipStat(count, avg));
                        } else if (key.startsWith("BORO|")) {
                            String boro = key.substring("BORO|".length());
                            boroCounts.put(boro, val);
                        } else if (key.startsWith("BOROCD|")) {
                            String rest = key.substring("BOROCD|".length());
                            boroCuisineAvg.put(rest, val);
                        }
                    }
                } finally {
                    br.close();
                }
            }
        }
    }

    private static class ZipStat {
        final String count;
        final String avgScore;
        // Tworzy prosty obiekt przechowujący liczbę restauracji w ZIP oraz średni SCORE dla ZIP.
        ZipStat(String count, String avgScore) {
            this.count = count;
            this.avgScore = avgScore;
        }
    }

    // Zapisuje w konfiguracji Hadoop indeksy kolumn DOHMH znalezione w nagłówku wejściowego CSV.
    private static void setDohmhColumns(Configuration conf, Map<String, Integer> h) {
        conf.setInt("dohmh.idx.camis", firstExisting(h, 0, "CAMIS"));
        conf.setInt("dohmh.idx.boro", firstExisting(h, 2, "BORO", "BOROUGH"));
        conf.setInt("dohmh.idx.building", firstExisting(h, 3, "BUILDING"));
        conf.setInt("dohmh.idx.street", firstExisting(h, 4, "STREET"));
        conf.setInt("dohmh.idx.zipcode", firstExisting(h, 5, "ZIPCODE", "ZIP CODE"));
        conf.setInt("dohmh.idx.cuisine", firstExisting(h, 7, "CUISINE DESCRIPTION", "CUISINE_DESCRIPTION"));
        conf.setInt("dohmh.idx.inspection_date", firstExisting(h, 8, "INSPECTION DATE", "INSPECTION_DATE"));
        conf.setInt("dohmh.idx.score", firstExisting(h, 13, "SCORE"));
        conf.setInt("dohmh.idx.bbl", firstExisting(h, 24, "BBL"));
    }

    // Zapisuje w konfiguracji Hadoop indeksy kolumn PLUTO znalezione w nagłówku wejściowego CSV.
    private static void setPlutoColumns(Configuration conf, Map<String, Integer> h) {
        conf.setInt("pluto.idx.address", firstExisting(h, 14, "ADDRESS"));
        conf.setInt("pluto.idx.postcode", firstExisting(h, 8, "POSTCODE", "ZIPCODE", "ZIP CODE"));
        conf.setInt("pluto.idx.bbl", firstExisting(h, 68, "BBL"));
        conf.setInt("pluto.idx.landuse", firstExisting(h, 27, "LANDUSE", "LAND USE"));
        conf.setInt("pluto.idx.yearbuilt", firstExisting(h, 58, "YEARBUILT", "YEAR BUILT"));
    }

    // Czyta pierwszy wiersz pliku CSV z HDFS i buduje mapę: kanoniczna nazwa kolumny -> indeks kolumny.
    // Wejście: konfiguracja Hadoop oraz ścieżka HDFS do pliku CSV.
    // Wyjście: mapa indeksów używana później do odpornego odczytu kolumn niezależnie od ich kolejności.
    private static Map<String, Integer> readHeaderIndex(Configuration conf, Path inputPath) throws IOException {
        FileSystem fs = inputPath.getFileSystem(conf);
        FSDataInputStream in = fs.open(inputPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        try {
            String firstLine = br.readLine();
            Map<String, Integer> out = new HashMap<String, Integer>();
            if (firstLine == null) {
                return out;
            }
            List<String> headers = parseCsvLine(firstLine);
            for (int i = 0; i < headers.size(); i++) {
                out.put(canon(headers.get(i)), i);
            }
            return out;
        } finally {
            br.close();
        }
    }

    // Zwraca indeks pierwszej znalezionej kolumny spośród podanych możliwych nazw.
    // Wejście: mapa nagłówków, indeks domyślny i lista akceptowanych nazw kolumn.
    // Wyjście: indeks kolumny albo wartość domyślna, gdy kolumna nie została znaleziona.
    private static int firstExisting(Map<String, Integer> h, int defaultIndex, String... names) {
        for (String n : names) {
            Integer idx = h.get(canon(n));
            if (idx != null) {
                return idx.intValue();
            }
        }
        return defaultIndex;
    }

    // Wyszukuje pierwszy plik wynikowy part- w katalogu wyjściowym joba Hadoop.
    // Wejście: FileSystem HDFS oraz katalog z wynikiem MapReduce.
    // Wyjście: ścieżka do pliku part- lub null, jeżeli Hadoop nie utworzył pliku wynikowego.
    private static Path findFirstPartFile(FileSystem fs, Path dir) throws IOException {
        FileStatus[] statuses = fs.listStatus(dir);
        for (FileStatus st : statuses) {
            if (st.isFile() && st.getPath().getName().startsWith("part-")) {
                return st.getPath();
            }
        }
        return null;
    }

    // Kopiuje plik w obrębie HDFS strumieniowo blokami bajtów.
    // Wejście: FileSystem HDFS, ścieżka źródłowa i ścieżka docelowa.
    // Wyjście: nowy plik docelowy, zwykle wersjonowany CSV albo plik _latest.csv.
    private static void copyHdfsFile(FileSystem fs, Path src, Path dst) throws IOException {
        FSDataInputStream in = fs.open(src);
        FSDataOutputStream out = fs.create(dst, true);
        try {
            byte[] buffer = new byte[1024 * 1024];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read > 0) {
                    out.write(buffer, 0, read);
                }
            }
        } finally {
            try { in.close(); } catch (Exception ignored) { }
            try { out.close(); } catch (Exception ignored) { }
        }
    }

    // Parsuje pojedynczy wiersz CSV bez zewnętrznych bibliotek, z obsługą cudzysłowów, przecinków w polach i podwójnych cudzysłowów.
    // Wejście: surowy wiersz tekstowy CSV.
    // Wyjście: lista pól CSV w kolejności z pliku.
    private static List<String> parseCsvLine(String line) {
        List<String> result = new ArrayList<String>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                result.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        result.add(cur.toString());
        return result;
    }

    // Sprawdza, czy sparsowany wiersz wygląda jak nagłówek CSV, porównując pierwszą kolumnę z oczekiwaną nazwą.
    private static boolean isCsvHeader(List<String> f, String expectedFirstColumn) {
        if (f == null || f.isEmpty()) {
            return false;
        }
        return canon(f.get(0)).equals(canon(expectedFirstColumn));
    }

    // Bezpiecznie pobiera wartość pola z listy, zwracając pusty string, gdy indeks jest poza zakresem.
    private static String get(List<String> fields, int idx) {
        return idx >= 0 && idx < fields.size() ? fields.get(idx) : "";
    }

    // Normalizuje podstawowo tekst: obsługuje null, usuwa białe znaki z końców i usuwa znak BOM.
    private static String clean(String s) {
        if (s == null) {
            return "";
        }
        return s.trim().replace('\uFEFF', ' ');
    }

    // Zwraca oczyszczony tekst w uppercase z Locale.ROOT, aby nazwy były stabilne między systemami.
    private static String upper(String s) {
        return clean(s).toUpperCase(Locale.ROOT);
    }

    // Sprawdza, czy wartość jest nullem albo pustym tekstem po trim().
    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    // Rozpoznaje techniczne daty 1900, które oznaczają rekordy do usunięcia przed dalszą transformacją.
    private static boolean isInspectionDate1900(String s) {
        String x = clean(s).toUpperCase(Locale.ROOT);
        return x.equals("01/01/1900")
                || x.equals("1900-01-01T00:00:00.000")
                || x.startsWith("1900-01-01");
    }

    // Usuwa z tekstu wszystkie znaki poza cyframi, używane m.in. dla ZIP, BBL i roku budowy.
    private static String onlyDigits(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isDigit(c)) {
                out.append(c);
            }
        }
        return out.toString();
    }

    // Zwraca pierwsze pięć cyfr z tekstu, co standaryzuje ZIP/MODZCTA do postaci pięciocyfrowej.
    private static String firstFiveDigits(String s) {
        String d = onlyDigits(s);
        return d.length() >= 5 ? d.substring(0, 5) : d;
    }

    // Czyści i waliduje BBL; zwraca pustą wartość dla placeholderów i niepełnych identyfikatorów działki.
    private static String normalizeBbl(String s) {
        String d = onlyDigits(s);
        if (d.length() != 10) {
            return "";
        }

        boolean allZero = true;
        for (int i = 0; i < d.length(); i++) {
            if (d.charAt(i) != '0') {
                allZero = false;
                break;
            }
        }
        if (allZero) {
            return "";
        }

        
        boolean missingBlockAndLot = true;
        for (int i = 1; i < d.length(); i++) {
            if (d.charAt(i) != '0') {
                missingBlockAndLot = false;
                break;
            }
        }
        return missingBlockAndLot ? "" : d;
    }

    // Buduje klucz joinu DOHMH-PLUTO: preferuje BBL, a w razie braku używa ZIP+znormalizowanego adresu.
    private static String makeJoinKey(String zip, String bbl, String normalizedAddress) {
        String cleanBbl = normalizeBbl(bbl);
        if (!isBlank(cleanBbl)) {
            return "B|" + cleanBbl;
        }

        String cleanZip = firstFiveDigits(zip);
        if (isBlank(cleanZip) || isBlank(normalizedAddress)) {
            return "";
        }
        return "A|" + cleanZip + "|" + normalizedAddress;
    }

    // Usuwa sufiksy ST/ND/RD/TH z numerów ulic, np. 5TH -> 5, aby poprawić dopasowanie adresów.
    private static String stripOrdinalSuffix(String t) {
        if (t == null) {
            return "";
        }
        if (t.matches("[0-9]+(ST|ND|RD|TH)")) {
            return t.replaceAll("(ST|ND|RD|TH)$", "");
        }
        return t;
    }

    // Standaryzuje adres: uppercase, usunięcie znaków specjalnych, redukcja spacji i zamiana pełnych nazw ulic na skróty.
    private static String normalizeAddress(String address) {
        String x = upper(address);
        x = x.replace('.', ' ');
        x = x.replace(',', ' ');
        x = x.replace('#', ' ');
        x = x.replace('/', ' ');
        x = x.replaceAll("[^A-Z0-9\\-\\s]", " ");
        x = x.replaceAll("\\s+", " ").trim();
        if (x.isEmpty()) {
            return "";
        }

        String[] tokens = x.split(" ");
        List<String> out = new ArrayList<String>();
        for (String t : tokens) {
            if (t.length() == 0) {
                continue;
            }
            out.add(abbreviateAddressToken(stripOrdinalSuffix(t)));
        }
        return joinWithSpace(out);
    }

    // Zamienia pojedynczy token adresu na skrót, np. STREET -> ST, AVENUE -> AVE, NORTH -> N.
    private static String abbreviateAddressToken(String t) {
        if ("SAINT".equals(t)) return "ST";
        if ("STREET".equals(t) || "STR".equals(t)) return "ST";
        if ("AVENUE".equals(t) || "AV".equals(t)) return "AVE";
        if ("ROAD".equals(t)) return "RD";
        if ("BOULEVARD".equals(t)) return "BLVD";
        if ("DRIVE".equals(t)) return "DR";
        if ("LANE".equals(t)) return "LN";
        if ("COURT".equals(t)) return "CT";
        if ("PLACE".equals(t)) return "PL";
        if ("TERRACE".equals(t)) return "TER";
        if ("PARKWAY".equals(t)) return "PKWY";
        if ("HIGHWAY".equals(t)) return "HWY";
        if ("EXPRESSWAY".equals(t)) return "EXPY";
        if ("SQUARE".equals(t)) return "SQ";
        if ("CIRCLE".equals(t)) return "CIR";
        if ("NORTH".equals(t)) return "N";
        if ("SOUTH".equals(t)) return "S";
        if ("EAST".equals(t)) return "E";
        if ("WEST".equals(t)) return "W";
        return t;
    }

    // Łączy tokeny adresu pojedynczą spacją po zakończeniu normalizacji.
    private static String joinWithSpace(List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(' ');
            sb.append(values.get(i));
        }
        return sb.toString();
    }

    // Łączy pola wewnętrznym separatorem US, aby nie kolidować z przecinkami CSV.
    private static String joinUS(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(US);
            sb.append(values[i] == null ? "" : values[i].replace(US, " "));
        }
        return sb.toString();
    }

    // Rozdziela wewnętrzny rekord zapisany separatorem US na tablicę pól.
    private static String[] splitUS(String line) {
        return line.split(US, -1);
    }

    // Buduje poprawny wiersz CSV z tablicy wartości, używając csvEscape dla pól wymagających cudzysłowów.
    private static String toCsv(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(csvEscape(values[i]));
        }
        return sb.toString();
    }

    // Escapuje pojedynczą wartość CSV: dodaje cudzysłowy i podwaja cudzysłowy wewnątrz pola, jeśli jest to potrzebne.
    private static String csvEscape(String s) {
        if (s == null) {
            s = "";
        }
        boolean mustQuote = s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
        if (!mustQuote) {
            return s;
        }
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    // Usuwa znak pionowej kreski z pola pomocniczego, aby nie psuł wewnętrznego formatu |.
    private static String safePipe(String s) {
        return s == null ? "" : s.replace('|', ' ').trim();
    }

    // Tworzy kanoniczną nazwę kolumny: uppercase, zamiana podkreśleń na spacje i redukcja wielu spacji.
    private static String canon(String s) {
        if (s == null) return "";
        return s.trim().toUpperCase(Locale.ROOT).replace('_', ' ').replaceAll("\\s+", " ");
    }

    // Bezpiecznie konwertuje tekst na Double, zwracając null dla wartości pustych lub niepoprawnych.
    private static Double parseDouble(String s) {
        try {
            if (s == null || s.trim().isEmpty()) {
                return null;
            }
            return Double.valueOf(s.trim());
        } catch (Exception e) {
            return null;
        }
    }

    // Bezpiecznie konwertuje tekst na Integer, zwracając null dla wartości pustych lub niepoprawnych.
    private static Integer parseInt(String s) {
        try {
            if (s == null || s.trim().isEmpty()) {
                return null;
            }
            return Integer.valueOf(s.trim());
        } catch (Exception e) {
            return null;
        }
    }

    // Konwertuje offset z pliku na long; przy błędzie zwraca Long.MAX_VALUE, aby taki rekord nie wygrał jako pierwszy.
    private static long parseLongOrMax(String s) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return Long.MAX_VALUE;
        }
    }

    // Formatuje SCORE jako liczbę całkowitą, jeżeli nie ma części ułamkowej, w innym razie jako liczbę z czterema miejscami.
    private static String formatScore(Double score) {
        if (score == null) {
            return "";
        }
        if (Math.floor(score.doubleValue()) == score.doubleValue()) {
            return String.valueOf(score.longValue());
        }
        return formatDouble(score.doubleValue());
    }

    // Formatuje liczbę zmiennoprzecinkową do czterech miejsc po przecinku z separatorem kropki.
    private static String formatDouble(double v) {
        return String.format(Locale.US, "%.4f", v);
    }

    // Pobiera wartość z mapy albo zwraca pusty string, gdy klucz nie istnieje.
    private static String getOrEmpty(Map<String, String> map, String key) {
        String v = map.get(key);
        return v == null ? "" : v;
    }

    // Zwraca największą wartość z listy indeksów, aby szybko sprawdzić minimalną wymaganą długość wiersza.
    private static int max(int... values) {
        int m = Integer.MIN_VALUE;
        for (int v : values) {
            if (v > m) m = v;
        }
        return m;
    }
}

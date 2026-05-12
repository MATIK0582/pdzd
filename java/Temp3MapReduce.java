import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;















public class Temp3MapReduce extends Configured implements Tool {

    private static final String US = "\u001F"; 
    private static final String HEADER = "CAMIS,BORO,ZIPCODE,ADDRESS,CUISINE_DESCRIPTION,YEARBUILT,LANDUSE,CRIME_INSPECTION_RISK_SCORE,BUILDING_AGE_SCORE,RESTAURANT_DENSITY_QUALITY_INDEX,CUISINE_RELATIVE_SCORE,DOMINANT_CRIME_TYPE";

    // Punkt wejścia programu uruchamianego przez yarn jar.
    // Wejście: argumenty CLI przekazane do klasy MapReduce.
    // Wyjście: kod zakończenia procesu zwrócony przez ToolRunner.
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Temp3MapReduce(), args);
        System.exit(exitCode);
    }

    // Steruje pełnym przebiegiem etapu MapReduce: odczytuje argumenty, konfiguruje zadania Hadoop, uruchamia je w kolejności oraz zapisuje pliki wynikowe.
    // Wejście: ścieżki HDFS przekazane w argumentach lub wartości domyślne zapisane w kodzie.
    // Wyjście: plik CSV z timestampem, plik _latest.csv oraz kod statusu 0/1/2/... zależny od powodzenia poszczególnych jobów.
    @Override
    public int run(String[] args) throws Exception {
        String temp2Input = args.length > 0 ? args[0] : "/datasets/results/temp2_latest.csv";
        String resultsDir = args.length > 1 ? args[1] : "/datasets/results";

        Configuration baseConf = getConf();
        FileSystem fs = FileSystem.get(baseConf);
        fs.mkdirs(new Path(resultsDir));

        String ts = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        Path workDir = new Path(resultsDir + "/_temp3_work_" + ts);
        Path statsOut = new Path(workDir, "01_temp2_stats");
        Path finalOut = new Path(workDir, "02_final_temp3_dir");

        Path versionedFile = new Path(resultsDir + "/temp3_" + ts + ".csv");
        Path latestFile = new Path(resultsDir + "/temp3_latest.csv");

        if (fs.exists(workDir)) {
            fs.delete(workDir, true);
        }

        Map<String, Integer> temp2Header = readHeaderIndex(baseConf, new Path(temp2Input));

        Configuration statsConf = new Configuration(baseConf);
        setTemp2Columns(statsConf, temp2Header);
        Job statsJob = Job.getInstance(statsConf, "TEMP3 step 1 - calculate score and crime standard deviations");
        statsJob.setJarByClass(Temp3MapReduce.class);
        statsJob.setMapperClass(StatsMapper.class);
        statsJob.setReducerClass(StatsReducer.class);
        statsJob.setMapOutputKeyClass(Text.class);
        statsJob.setMapOutputValueClass(Text.class);
        statsJob.setOutputKeyClass(Text.class);
        statsJob.setOutputValueClass(Text.class);
        statsJob.setOutputFormatClass(TextOutputFormat.class);
        statsJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(statsJob, new Path(temp2Input));
        FileOutputFormat.setOutputPath(statsJob, statsOut);
        if (!statsJob.waitForCompletion(true)) {
            return 1;
        }

        Configuration finalConf = new Configuration(baseConf);
        setTemp2Columns(finalConf, temp2Header);
        finalConf.set("temp3.stats.path", statsOut.toString());
        Job finalJob = Job.getInstance(finalConf, "TEMP3 step 2 - create final TEMP3 dataset");
        finalJob.setJarByClass(Temp3MapReduce.class);
        finalJob.setMapperClass(FinalMapper.class);
        finalJob.setReducerClass(FinalReducer.class);
        finalJob.setMapOutputKeyClass(Text.class);
        finalJob.setMapOutputValueClass(Text.class);
        finalJob.setOutputKeyClass(NullWritable.class);
        finalJob.setOutputValueClass(Text.class);
        finalJob.setOutputFormatClass(TextOutputFormat.class);
        finalJob.setNumReduceTasks(1); 
        FileInputFormat.addInputPath(finalJob, new Path(temp2Input));
        FileOutputFormat.setOutputPath(finalJob, finalOut);
        if (!finalJob.waitForCompletion(true)) {
            return 2;
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

        System.out.println("TEMP3 versioned output: " + versionedFile);
        System.out.println("TEMP3 latest output:    " + latestFile);
        System.out.println("Requested HDFS replication factor: 3");
        return 0;
    }

    
    public static class StatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int idxZip;
        private int idxScore;
        private int idxCuisine;
        private int idxBoro;
        private int idxCountCrimePerZip;
        private int maxIdx;
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        // Pobiera indeksy kolumn TEMP2 potrzebne do policzenia odchyleń score i crime.
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            idxZip = conf.getInt("temp2.idx.zipcode", 0);
            idxScore = conf.getInt("temp2.idx.score", 2);
            idxCuisine = conf.getInt("temp2.idx.cuisine", 3);
            idxBoro = conf.getInt("temp2.idx.boro", 5);
            idxCountCrimePerZip = conf.getInt("temp2.idx.count_crime_per_zip", 17);
            maxIdx = max(idxZip, idxScore, idxCuisine, idxBoro, idxCountCrimePerZip);
        }

        // Tworzy rekordy pomocnicze do policzenia STDEV_SCORE_ZIP, STDEV_SCORE_BORO_CD oraz crime count per ZIP.
        // Wejście: wiersz temp2_latest.csv.
        // Wyjście: pary ZIP_SCORE|ZIP, BOROCD_SCORE|BORO|CUISINE oraz ZIP_CRIME|ZIP.
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) {
                return;
            }
            List<String> f = parseCsvLine(line);
            if (isCsvHeader(f, "ZIPCODE")) {
                return;
            }
            if (f.size() <= maxIdx) {
                return;
            }

            String zip = firstFiveDigits(clean(get(f, idxZip)));
            String boro = upper(get(f, idxBoro));
            String cuisine = upper(get(f, idxCuisine));
            String scoreRaw = clean(get(f, idxScore));
            String crimeRaw = clean(get(f, idxCountCrimePerZip));

            Double score = parseDouble(scoreRaw);
            if (!isBlank(zip) && score != null) {
                outKey.set("ZIP_SCORE|" + zip);
                outValue.set(formatDoublePlain(score.doubleValue()));
                context.write(outKey, outValue);
            }

            if (!isBlank(boro) && !isBlank(cuisine) && score != null) {
                outKey.set("BOROCD_SCORE|" + boro + "|" + cuisine);
                outValue.set(formatDoublePlain(score.doubleValue()));
                context.write(outKey, outValue);
            }

            Double crimeCount = parseDouble(crimeRaw);
            if (!isBlank(zip) && crimeCount != null) {
                outKey.set("ZIP_CRIME|" + zip);
                outValue.set(formatDoublePlain(crimeCount.doubleValue()));
                context.write(outKey, outValue);
            }
        }
    }

    
    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {
        // Liczy populacyjne odchylenia standardowe SCORE dla ZIP i BORO+CUISINE oraz zapisuje jedną wartość crime count dla ZIP.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String k = key.toString();

            if (k.startsWith("ZIP_SCORE|") || k.startsWith("BOROCD_SCORE|")) {
                List<Double> scores = new ArrayList<Double>();
                for (Text v : values) {
                    Double score = parseDouble(v.toString());
                    if (score != null) {
                        scores.add(score);
                    }
                }
                context.write(key, new Text(formatDouble(populationStdev(scores))));
            } else if (k.startsWith("ZIP_CRIME|")) {
                String crimeCount = "0";
                for (Text v : values) {
                    Double count = parseDouble(v.toString());
                    if (count != null) {
                        crimeCount = formatDoublePlain(count.doubleValue());
                        break;
                    }
                }
                context.write(key, new Text(crimeCount));
            }
        }
    }

    
    public static class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {
        // Przekazuje wszystkie wiersze TEMP2 do jednego reducera, aby finalny plik miał jeden nagłówek i jeden part-file.
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) {
                return;
            }
            List<String> f = parseCsvLine(line);
            if (isCsvHeader(f, "ZIPCODE")) {
                return;
            }
            context.write(new Text("ALL"), new Text(line));
        }
    }

    
    public static class FinalReducer extends Reducer<Text, Text, NullWritable, Text> {
        private final Map<String, Double> stdevScoreByZip = new HashMap<String, Double>();
        private final Map<String, Double> stdevScoreByBoroCuisine = new HashMap<String, Double>();
        private final Map<String, Double> crimeCountByZip = new HashMap<String, Double>();

        private double stdevCrimePerZip = 0.0;

        private int idxZip;
        private int idxCamis;
        private int idxScore;
        private int idxCuisine;
        private int idxAddress;
        private int idxBoro;
        private int idxAvgScoreZip;
        private int idxAvgScoreBoroCd;
        private int idxRestaurantDensityQualityIndex;
        private int idxLanduse;
        private int idxBuildingAge;
        private int idxYearbuilt;
        private int idxAvgCrimePerZip;
        private int idxCountCrimePerZip;
        private int idxDominantCrimeType;
        private int maxIdx;

        // Pobiera indeksy kolumn TEMP2, ładuje statystyki z pierwszego joba i zapisuje nagłówek finalnego TEMP3.
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            idxZip = conf.getInt("temp2.idx.zipcode", 0);
            idxCamis = conf.getInt("temp2.idx.camis", 1);
            idxScore = conf.getInt("temp2.idx.score", 2);
            idxCuisine = conf.getInt("temp2.idx.cuisine", 3);
            idxAddress = conf.getInt("temp2.idx.address", 4);
            idxBoro = conf.getInt("temp2.idx.boro", 5);
            idxAvgScoreZip = conf.getInt("temp2.idx.avg_score_zip", 10);
            idxAvgScoreBoroCd = conf.getInt("temp2.idx.avg_score_boro_cd", 11);
            idxRestaurantDensityQualityIndex = conf.getInt("temp2.idx.restaurant_density_quality_index", 12);
            idxLanduse = conf.getInt("temp2.idx.landuse", 13);
            idxBuildingAge = conf.getInt("temp2.idx.building_age", 14);
            idxYearbuilt = conf.getInt("temp2.idx.yearbuilt", 15);
            idxAvgCrimePerZip = conf.getInt("temp2.idx.avg_crime_per_zip", 16);
            idxCountCrimePerZip = conf.getInt("temp2.idx.count_crime_per_zip", 17);
            idxDominantCrimeType = conf.getInt("temp2.idx.dominant_crime_type", 18);
            maxIdx = max(idxZip, idxCamis, idxScore, idxCuisine, idxAddress, idxBoro, idxAvgScoreZip,
                    idxAvgScoreBoroCd, idxRestaurantDensityQualityIndex, idxLanduse, idxBuildingAge,
                    idxYearbuilt, idxAvgCrimePerZip, idxCountCrimePerZip, idxDominantCrimeType);

            loadStats(conf);
            calculateCrimeStdev();
            context.write(NullWritable.get(), new Text(HEADER));
        }

        // Tworzy finalny, okrojony zbiór wynikowy TEMP3 z kolumnami analitycznymi i wybranymi kolumnami źródłowymi.
        // Wejście: wszystkie wiersze TEMP2 przekazane pod kluczem ALL.
        // Wyjście: finalne wiersze temp3 CSV, zwykle 1:1 względem wierszy TEMP2, o ile wiersz ma wymagane kolumny.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                List<String> f = parseCsvLine(v.toString());
                if (f.size() <= maxIdx) {
                    continue;
                }

                String zip = firstFiveDigits(clean(get(f, idxZip)));
                String camis = clean(get(f, idxCamis));
                String score = clean(get(f, idxScore));
                String cuisine = clean(get(f, idxCuisine));
                String cuisineKey = upper(cuisine);
                String address = clean(get(f, idxAddress));
                String boro = clean(get(f, idxBoro));
                String boroKey = upper(boro);
                String avgScoreZip = clean(get(f, idxAvgScoreZip));
                String avgScoreBoroCd = clean(get(f, idxAvgScoreBoroCd));
                String restaurantDensityQualityIndex = clean(get(f, idxRestaurantDensityQualityIndex));
                String landuse = clean(get(f, idxLanduse));
                String buildingAgeScore = clean(get(f, idxBuildingAge));
                String yearbuilt = clean(get(f, idxYearbuilt));
                String avgCrimePerZip = clean(get(f, idxAvgCrimePerZip));
                String countCrimePerZip = clean(get(f, idxCountCrimePerZip));
                String dominantCrimeType = clean(get(f, idxDominantCrimeType));

                String crimeInspectionRiskScore = calculateCrimeInspectionRiskScore(zip, score, avgScoreZip,
                        countCrimePerZip, avgCrimePerZip);
                String cuisineRelativeScore = calculateCuisineRelativeScore(boroKey, cuisineKey, score, avgScoreBoroCd);

                String[] csv = new String[] {
                        camis,
                        boro,
                        zip,
                        address,
                        cuisine,
                        yearbuilt,
                        landuse,
                        crimeInspectionRiskScore,
                        buildingAgeScore,
                        restaurantDensityQualityIndex,
                        cuisineRelativeScore,
                        dominantCrimeType
                };
                context.write(NullWritable.get(), new Text(toCsv(csv)));
            }
        }

        // Wczytuje odchylenia standardowe SCORE oraz crime count per ZIP z wyniku joba statystycznego.
        private void loadStats(Configuration conf) throws IOException {
            String statsPath = conf.get("temp3.stats.path");
            if (statsPath == null) {
                throw new IOException("Missing temp3.stats.path configuration");
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
                        String k = line.substring(0, tab);
                        String val = line.substring(tab + 1);
                        Double parsed = parseDouble(val);

                        if (k.startsWith("ZIP_SCORE|")) {
                            String zip = k.substring("ZIP_SCORE|".length());
                            if (parsed != null) {
                                stdevScoreByZip.put(zip, parsed);
                            }
                        } else if (k.startsWith("BOROCD_SCORE|")) {
                            String boroCuisine = k.substring("BOROCD_SCORE|".length());
                            if (parsed != null) {
                                stdevScoreByBoroCuisine.put(boroCuisine, parsed);
                            }
                        } else if (k.startsWith("ZIP_CRIME|")) {
                            String zip = k.substring("ZIP_CRIME|".length());
                            if (parsed != null) {
                                crimeCountByZip.put(zip, parsed);
                            }
                        }
                    }
                } finally {
                    br.close();
                }
            }
        }

        // Liczy STDEV_CRIME_PER_ZIP z unikalnych wartości COUNT_CRIME_PER_ZIP po ZIP.
        private void calculateCrimeStdev() {
            List<Double> values = new ArrayList<Double>(crimeCountByZip.values());
            stdevCrimePerZip = populationStdev(values);
        }

        // Liczy CRIME_INSPECTION_RISK_SCORE = NORM_SCORE_ZIP + NORM_CRIME_ZIP według finalnego wzoru projektu.
        private String calculateCrimeInspectionRiskScore(String zip, String scoreRaw, String avgScoreZipRaw,
                                                         String countCrimeRaw, String avgCrimeRaw) {
            Double score = parseDouble(scoreRaw);
            Double avgScoreZip = parseDouble(avgScoreZipRaw);
            Double countCrime = parseDouble(countCrimeRaw);
            Double avgCrime = parseDouble(avgCrimeRaw);
            Double stdevScoreZip = stdevScoreByZip.get(zip);

            if (score == null || avgScoreZip == null || countCrime == null || avgCrime == null || stdevScoreZip == null) {
                return "";
            }

            double normScoreZip = stdevScoreZip.doubleValue() == 0.0
                    ? 0.0
                    : (score.doubleValue() - avgScoreZip.doubleValue()) / stdevScoreZip.doubleValue();
            double normCrimeZip = stdevCrimePerZip == 0.0
                    ? 0.0
                    : (countCrime.doubleValue() - avgCrime.doubleValue()) / stdevCrimePerZip;

            return formatDouble(normScoreZip + normCrimeZip);
        }

        // Liczy CUISINE_RELATIVE_SCORE jako z-score SCORE względem średniej i odchylenia grupy BORO+CUISINE.
        private String calculateCuisineRelativeScore(String boro, String cuisine, String scoreRaw, String avgScoreBoroCdRaw) {
            Double score = parseDouble(scoreRaw);
            Double avgScoreBoroCd = parseDouble(avgScoreBoroCdRaw);
            Double stdev = stdevScoreByBoroCuisine.get(boro + "|" + cuisine);

            if (score == null || avgScoreBoroCd == null || stdev == null) {
                return "";
            }
            if (stdev.doubleValue() == 0.0) {
                return formatDouble(0.0);
            }

            
            return formatDouble((score.doubleValue() - avgScoreBoroCd.doubleValue()) / stdev.doubleValue());
        }
    }

    // Zapisuje w konfiguracji indeksy kolumn TEMP2 używanych w finalnej transformacji.
    private static void setTemp2Columns(Configuration conf, Map<String, Integer> h) {
        conf.setInt("temp2.idx.zipcode", firstExisting(h, 0, "ZIPCODE", "ZIP CODE"));
        conf.setInt("temp2.idx.camis", firstExisting(h, 1, "CAMIS"));
        conf.setInt("temp2.idx.score", firstExisting(h, 2, "SCORE"));
        conf.setInt("temp2.idx.cuisine", firstExisting(h, 3, "CUISINE_DESCRIPTION", "CUISINE DESCRIPTION"));
        conf.setInt("temp2.idx.address", firstExisting(h, 4, "ADDRESS"));
        conf.setInt("temp2.idx.boro", firstExisting(h, 5, "BORO", "BOROUGH"));
        conf.setInt("temp2.idx.avg_score_zip", firstExisting(h, 10, "AVG_SCORE_ZIP", "AVG SCORE ZIP"));
        conf.setInt("temp2.idx.avg_score_boro_cd", firstExisting(h, 11, "AVG_SCORE_BORO_CD", "AVG SCORE BORO CD"));
        conf.setInt("temp2.idx.restaurant_density_quality_index", firstExisting(h, 12, "RESTAURANT_DENSITY_QUALITY_INDEX", "RESTAURANT DENSITY QUALITY INDEX"));
        conf.setInt("temp2.idx.landuse", firstExisting(h, 13, "LANDUSE", "LAND USE"));
        conf.setInt("temp2.idx.building_age", firstExisting(h, 14, "BUILDING_AGE", "BUILDING AGE", "BUILDING_AGE_SCORE", "BUILDING AGE SCORE"));
        conf.setInt("temp2.idx.yearbuilt", firstExisting(h, 15, "YEARBUILT", "YEAR BUILT", "YEARBUIT"));
        conf.setInt("temp2.idx.avg_crime_per_zip", firstExisting(h, 16, "AVG_CRIME_PER_ZIP", "AVG CRIME PER ZIP"));
        conf.setInt("temp2.idx.count_crime_per_zip", firstExisting(h, 17, "COUNT_CRIME_PER_ZIP", "COUNT CRIME PER ZIP"));
        conf.setInt("temp2.idx.dominant_crime_type", firstExisting(h, 18, "DOMINANT_CRIME_TYPE", "DOMINANT CRIME TYPE"));
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

    // Liczy średnią arytmetyczną z listy wartości liczbowych; dla pustej listy zwraca 0.0.
    private static double mean(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        int count = 0;
        for (Double v : values) {
            if (v != null) {
                sum += v.doubleValue();
                count++;
            }
        }
        return count == 0 ? 0.0 : sum / count;
    }

    // Liczy populacyjne odchylenie standardowe, czyli sqrt(sum((x-mean)^2)/n).
    // Wejście: lista wartości liczbowych traktowana jako pełna populacja danych dostępnych w projekcie.
    // Wyjście: odchylenie standardowe populacyjne; dla pustej listy 0.0.
    private static double populationStdev(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }
        double avg = mean(values);
        double sumSq = 0.0;
        int count = 0;
        for (Double v : values) {
            if (v != null) {
                double d = v.doubleValue() - avg;
                sumSq += d * d;
                count++;
            }
        }
        return count == 0 ? 0.0 : Math.sqrt(sumSq / count);
    }

    // Formatuje liczbę zmiennoprzecinkową do czterech miejsc po przecinku z separatorem kropki.
    private static String formatDouble(double v) {
        return String.format(Locale.US, "%.4f", v);
    }

    // Formatuje liczbę bez zbędnych zer po przecinku, gdy jest całkowita; inaczej używa czterech miejsc po przecinku.
    private static String formatDoublePlain(double v) {
        if (Math.floor(v) == v) {
            return String.valueOf((long) v);
        }
        return String.format(Locale.US, "%.10f", v);
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

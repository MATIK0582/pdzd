import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
















public class Temp2MapReduce extends Configured implements Tool {

    private static final String US = "\u001F"; 
    private static final String HEADER = "ZIPCODE,CAMIS,SCORE,CUISINE_DESCRIPTION,ADDRESS,BORO,NUMBER_PER_ZIP,STDEV_PER_ZIP,NUMBER_PER_BORO,STDEV_PER_BORO,AVG_SCORE_ZIP,AVG_SCORE_BORO_CD,RESTAURANT_DENSITY_QUALITY_INDEX,LANDUSE,BUILDING_AGE,YEARBUILT,AVG_CRIME_PER_ZIP,COUNT_CRIME_PER_ZIP,DOMINANT_CRIME_TYPE,CRIME_INSPECTION_RISK_SCORE";

    // Punkt wejścia programu uruchamianego przez yarn jar.
    // Wejście: argumenty CLI przekazane do klasy MapReduce.
    // Wyjście: kod zakończenia procesu zwrócony przez ToolRunner.
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Temp2MapReduce(), args);
        System.exit(exitCode);
    }

    // Steruje pełnym przebiegiem etapu MapReduce: odczytuje argumenty, konfiguruje zadania Hadoop, uruchamia je w kolejności oraz zapisuje pliki wynikowe.
    // Wejście: ścieżki HDFS przekazane w argumentach lub wartości domyślne zapisane w kodzie.
    // Wyjście: plik CSV z timestampem, plik _latest.csv oraz kod statusu 0/1/2/... zależny od powodzenia poszczególnych jobów.
    @Override
    public int run(String[] args) throws Exception {
        String temp1Input = args.length > 0 ? args[0] : "/datasets/results/temp1_latest.csv";
        String nypdZipInput = args.length > 1 ? args[1] : "/datasets/results/nypd_zip_latest.csv";
        String resultsDir = args.length > 2 ? args[2] : "/datasets/results";

        Configuration baseConf = getConf();
        FileSystem fs = FileSystem.get(baseConf);
        fs.mkdirs(new Path(resultsDir));

        String ts = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        Path workDir = new Path(resultsDir + "/_temp2_work_" + ts);
        Path temp1StatsOut = new Path(workDir, "01_temp1_stats");
        Path crimeOut = new Path(workDir, "02_crime_by_zip");
        Path finalOut = new Path(workDir, "03_final_temp2_dir");

        Path versionedFile = new Path(resultsDir + "/temp2_" + ts + ".csv");
        Path latestFile = new Path(resultsDir + "/temp2_latest.csv");

        if (fs.exists(workDir)) {
            fs.delete(workDir, true);
        }

        Map<String, Integer> temp1Header = readHeaderIndex(baseConf, new Path(temp1Input));
        Map<String, Integer> nypdHeader = readHeaderIndex(baseConf, new Path(nypdZipInput));

        Configuration statsConf = new Configuration(baseConf);
        setTemp1Columns(statsConf, temp1Header);
        Job statsJob = Job.getInstance(statsConf, "TEMP2 step 1 - TEMP1 ZIP/BORO statistics");
        statsJob.setJarByClass(Temp2MapReduce.class);
        statsJob.setMapperClass(Temp1StatsMapper.class);
        statsJob.setReducerClass(Temp1StatsReducer.class);
        statsJob.setMapOutputKeyClass(Text.class);
        statsJob.setMapOutputValueClass(Text.class);
        statsJob.setOutputKeyClass(Text.class);
        statsJob.setOutputValueClass(Text.class);
        statsJob.setOutputFormatClass(TextOutputFormat.class);
        statsJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(statsJob, new Path(temp1Input));
        FileOutputFormat.setOutputPath(statsJob, temp1StatsOut);
        if (!statsJob.waitForCompletion(true)) {
            return 1;
        }

        Configuration crimeConf = new Configuration(baseConf);
        setNypdZipColumns(crimeConf, nypdHeader);
        Job crimeJob = Job.getInstance(crimeConf, "TEMP2 step 2 - aggregate NYPD crime by ZIP");
        crimeJob.setJarByClass(Temp2MapReduce.class);
        crimeJob.setMapperClass(NypdCrimeMapper.class);
        crimeJob.setReducerClass(NypdCrimeReducer.class);
        crimeJob.setMapOutputKeyClass(Text.class);
        crimeJob.setMapOutputValueClass(Text.class);
        crimeJob.setOutputKeyClass(Text.class);
        crimeJob.setOutputValueClass(Text.class);
        crimeJob.setOutputFormatClass(TextOutputFormat.class);
        crimeJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(crimeJob, new Path(nypdZipInput));
        FileOutputFormat.setOutputPath(crimeJob, crimeOut);
        if (!crimeJob.waitForCompletion(true)) {
            return 2;
        }

        Configuration finalConf = new Configuration(baseConf);
        setTemp1Columns(finalConf, temp1Header);
        finalConf.set("temp2.temp1.stats.path", temp1StatsOut.toString());
        finalConf.set("temp2.crime.stats.path", crimeOut.toString());
        finalConf.setInt("temp2.current.year", getCurrentYear());
        Job finalJob = Job.getInstance(finalConf, "TEMP2 step 3 - enrich TEMP1 with crime and calculated metrics");
        finalJob.setJarByClass(Temp2MapReduce.class);
        finalJob.setMapperClass(FinalTemp1Mapper.class);
        finalJob.setReducerClass(FinalReducer.class);
        finalJob.setMapOutputKeyClass(Text.class);
        finalJob.setMapOutputValueClass(Text.class);
        finalJob.setOutputKeyClass(NullWritable.class);
        finalJob.setOutputValueClass(Text.class);
        finalJob.setOutputFormatClass(TextOutputFormat.class);
        finalJob.setNumReduceTasks(1); 
        FileInputFormat.addInputPath(finalJob, new Path(temp1Input));
        FileOutputFormat.setOutputPath(finalJob, finalOut);
        if (!finalJob.waitForCompletion(true)) {
            return 3;
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

        System.out.println("TEMP2 versioned output: " + versionedFile);
        System.out.println("TEMP2 latest output:    " + latestFile);
        System.out.println("Requested HDFS replication factor: 3");
        return 0;
    }

    
    public static class Temp1StatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int idxZip;
        private int idxBoro;
        private int idxNumberPerZip;
        private int idxNumberPerBoro;
        private int idxAvgScoreZip;
        private int maxIdx;
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        // Pobiera indeksy kolumn TEMP1 potrzebne do policzenia statystyk ZIP i BORO.
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            idxZip = conf.getInt("temp1.idx.zipcode", 0);
            idxBoro = conf.getInt("temp1.idx.boro", 5);
            idxNumberPerZip = conf.getInt("temp1.idx.number_per_zip", 6);
            idxNumberPerBoro = conf.getInt("temp1.idx.number_per_boro", 7);
            idxAvgScoreZip = conf.getInt("temp1.idx.avg_score_zip", 8);
            maxIdx = max(idxZip, idxBoro, idxNumberPerZip, idxNumberPerBoro, idxAvgScoreZip);
        }

        // Wyciąga z TEMP1 unikalne informacje o ZIP i BORO potrzebne do odchyleń standardowych oraz średnich.
        // Wejście: wiersz temp1_latest.csv.
        // Wyjście: rekordy ZIP|... z NUMBER_PER_ZIP/AVG_SCORE_ZIP oraz BORO|... z NUMBER_PER_BORO.
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
            String numberPerZip = clean(get(f, idxNumberPerZip));
            String numberPerBoro = clean(get(f, idxNumberPerBoro));
            String avgScoreZip = clean(get(f, idxAvgScoreZip));

            if (!isBlank(zip) && parseDouble(numberPerZip) != null) {
                outKey.set("ZIP|" + zip);
                outValue.set(safePipe(numberPerZip) + "|" + safePipe(avgScoreZip));
                context.write(outKey, outValue);
            }

            if (!isBlank(boro) && parseDouble(numberPerBoro) != null) {
                outKey.set("BORO|" + boro);
                outValue.set(safePipe(numberPerBoro));
                context.write(outKey, outValue);
            }
        }
    }

    
    public static class Temp1StatsReducer extends Reducer<Text, Text, Text, Text> {
        // Redukuje powtarzające się rekordy TEMP1 do jednego rekordu statystycznego per ZIP albo per BORO.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String k = key.toString();
            if (k.startsWith("ZIP|")) {
                String numberPerZip = "";
                String avgScoreZip = "";
                for (Text v : values) {
                    String[] p = v.toString().split("\\|", -1);
                    if (p.length > 0 && isBlank(numberPerZip) && parseDouble(p[0]) != null) {
                        numberPerZip = p[0];
                    }
                    if (p.length > 1 && isBlank(avgScoreZip) && parseDouble(p[1]) != null) {
                        avgScoreZip = p[1];
                    }
                    if (!isBlank(numberPerZip) && !isBlank(avgScoreZip)) {
                        break;
                    }
                }
                context.write(key, new Text(numberPerZip + "|" + avgScoreZip));
            } else if (k.startsWith("BORO|")) {
                String numberPerBoro = "";
                for (Text v : values) {
                    String candidate = v.toString();
                    if (parseDouble(candidate) != null) {
                        numberPerBoro = candidate;
                        break;
                    }
                }
                context.write(key, new Text(numberPerBoro));
            }
        }
    }

    
    public static class NypdCrimeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int idxCmplntNum;
        private int idxZip;
        private int idxOfnsDesc;
        private int maxIdx;
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        // Pobiera indeksy kolumn wzbogaconego NYPD_ZIP: CMPLNT_NUM, ZIPCODE i OFNS_DESC.
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            idxCmplntNum = conf.getInt("nypd.idx.cmplnt_num", 0);
            idxZip = conf.getInt("nypd.idx.zipcode", 1);
            idxOfnsDesc = conf.getInt("nypd.idx.ofns_desc", 2);
            maxIdx = max(idxCmplntNum, idxZip, idxOfnsDesc);
        }

        // Mapuje pojedynczy rekord nypd_zip_latest.csv do ZIP-a, zachowując numer zgłoszenia i typ przestępstwa.
        // Wejście: rekord NYPD po przypisaniu ZIP.
        // Wyjście: para ZIP -> CMPLNT_NUM|OFNS_DESC; puste ZIP/CMPLNT_NUM/OFNS_DESC są pomijane.
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) {
                return;
            }
            List<String> f = parseCsvLine(line);
            if (isCsvHeader(f, "CMPLNT_NUM")) {
                return;
            }
            if (f.size() <= maxIdx) {
                return;
            }

            String cmplntNum = clean(get(f, idxCmplntNum));
            String zip = firstFiveDigits(clean(get(f, idxZip)));
            String ofnsDesc = upper(get(f, idxOfnsDesc));

            if (isBlank(cmplntNum) || isBlank(zip) || isBlank(ofnsDesc)) {
                return;
            }

            outKey.set(zip);
            outValue.set(safePipe(cmplntNum) + "|" + safePipe(ofnsDesc));
            context.write(outKey, outValue);
        }
    }

    
    public static class NypdCrimeReducer extends Reducer<Text, Text, Text, Text> {
        // Liczy COUNT(DISTINCT CMPLNT_NUM) i dominujący typ przestępstwa dla każdego ZIP, z rozstrzyganiem remisów alfabetycznie.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> distinctComplaints = new HashSet<String>();
            Map<String, Integer> offenseCounts = new HashMap<String, Integer>();

            for (Text v : values) {
                String[] p = v.toString().split("\\|", 2);
                if (p.length < 2) {
                    continue;
                }
                String cmplntNum = p[0];
                String offense = p[1];
                if (isBlank(cmplntNum) || isBlank(offense)) {
                    continue;
                }
                if (distinctComplaints.add(cmplntNum)) {
                    Integer old = offenseCounts.get(offense);
                    offenseCounts.put(offense, old == null ? 1 : old.intValue() + 1);
                }
            }

            int count = distinctComplaints.size();
            String dominant = chooseDominantAlphabetically(offenseCounts);
            context.write(key, new Text(count + "|" + dominant));
        }
    }

    
    public static class FinalTemp1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private int idxZip;
        private int idxCamis;
        private int idxScore;
        private int idxCuisine;
        private int idxAddress;
        private int idxBoro;
        private int idxNumberPerZip;
        private int idxNumberPerBoro;
        private int idxAvgScoreZip;
        private int idxAvgScoreBoroCd;
        private int idxLanduse;
        private int idxYearbuilt;
        private int maxIdx;
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        // Pobiera indeksy wszystkich kolumn TEMP1 wymaganych do budowy TEMP2.
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            idxZip = conf.getInt("temp1.idx.zipcode", 0);
            idxCamis = conf.getInt("temp1.idx.camis", 1);
            idxScore = conf.getInt("temp1.idx.score", 2);
            idxCuisine = conf.getInt("temp1.idx.cuisine", 3);
            idxAddress = conf.getInt("temp1.idx.address", 4);
            idxBoro = conf.getInt("temp1.idx.boro", 5);
            idxNumberPerZip = conf.getInt("temp1.idx.number_per_zip", 6);
            idxNumberPerBoro = conf.getInt("temp1.idx.number_per_boro", 7);
            idxAvgScoreZip = conf.getInt("temp1.idx.avg_score_zip", 8);
            idxAvgScoreBoroCd = conf.getInt("temp1.idx.avg_score_boro_cd", 9);
            idxLanduse = conf.getInt("temp1.idx.landuse", 10);
            idxYearbuilt = conf.getInt("temp1.idx.yearbuilt", 11);
            maxIdx = max(idxZip, idxCamis, idxScore, idxCuisine, idxAddress, idxBoro, idxNumberPerZip,
                    idxNumberPerBoro, idxAvgScoreZip, idxAvgScoreBoroCd, idxLanduse, idxYearbuilt);
        }

        // Przepisuje rekord TEMP1 do wewnętrznego formatu i kluczuje go ZIP-em, aby reducer mógł dopisać statystyki crime.
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
            if (isBlank(zip)) {
                return;
            }

            String[] row = new String[] {
                    zip,
                    clean(get(f, idxCamis)),
                    clean(get(f, idxScore)),
                    clean(get(f, idxCuisine)),
                    clean(get(f, idxAddress)),
                    upper(get(f, idxBoro)),
                    clean(get(f, idxNumberPerZip)),
                    clean(get(f, idxNumberPerBoro)),
                    clean(get(f, idxAvgScoreZip)),
                    clean(get(f, idxAvgScoreBoroCd)),
                    clean(get(f, idxLanduse)),
                    clean(get(f, idxYearbuilt))
            };

            outKey.set(zip);
            outValue.set(joinUS(row));
            context.write(outKey, outValue);
        }
    }

    
    public static class FinalReducer extends Reducer<Text, Text, NullWritable, Text> {
        private final Map<String, Double> zipNumberPerZip = new HashMap<String, Double>();
        private final Map<String, Double> zipAvgScore = new HashMap<String, Double>();
        private double stdevPerBoro = 0.0;
        private final Map<String, Integer> crimeCountByZip = new HashMap<String, Integer>();
        private final Map<String, String> dominantCrimeByZip = new HashMap<String, String>();

        private double stdevPerZip = 0.0;
        private double meanCrimePerZip = 0.0;
        private double stdevCrimePerZip = 0.0;
        private double meanAvgScoreZip = 0.0;
        private double stdevAvgScoreZip = 0.0;
        private int currentYear;

        // Ładuje statystyki TEMP1 i NYPD do pamięci, liczy statystyki globalne oraz zapisuje nagłówek TEMP2.
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            currentYear = conf.getInt("temp2.current.year", getCurrentYear());
            loadTemp1Stats(conf);
            loadCrimeStats(conf);
            calculateGlobalStatistics();
            context.write(NullWritable.get(), new Text(HEADER));
        }

        // Dla każdego ZIP dopisuje do rekordów TEMP1 metryki TEMP2: odchylenia, indeks zagęszczenia, wiek budynku i statystyki crime.
        // Wejście: wszystkie wiersze TEMP1 z danego ZIP.
        // Wyjście: wiersze TEMP2 zachowujące kardynalność 1:1 względem TEMP1.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String zip = key.toString();
            int crimeCount = getCrimeCount(zip);
            String crimeCountOut = String.valueOf(crimeCount);
            String dominantCrime = getOrEmpty(dominantCrimeByZip, zip);
            String avgCrimeOut = formatDouble(meanCrimePerZip);

            for (Text v : values) {
                String[] r = splitUS(v.toString());
                if (r.length < 12) {
                    continue;
                }

                String rowZip = r[0];
                String camis = r[1];
                String score = r[2];
                String cuisine = r[3];
                String address = r[4];
                String boro = r[5];
                String numberPerZip = r[6];
                String numberPerBoro = r[7];
                String avgScoreZip = r[8];
                String avgScoreBoroCd = r[9];
                String landuse = r[10];
                String yearbuilt = r[11];

                String stdevPerZipOut = formatDouble(stdevPerZip);
                String stdevPerBoroOut = formatDouble(stdevPerBoro);
                String densityQualityIndex = calculateRestaurantDensityQualityIndex(numberPerZip, avgScoreZip);
                String buildingAge = calculateBuildingAge(yearbuilt, currentYear);
                String crimeInspectionRisk = calculateCrimeInspectionRisk(rowZip, avgScoreZip);

                String[] csv = new String[] {
                        rowZip,
                        camis,
                        score,
                        cuisine,
                        address,
                        boro,
                        numberPerZip,
                        stdevPerZipOut,
                        numberPerBoro,
                        stdevPerBoroOut,
                        avgScoreZip,
                        avgScoreBoroCd,
                        densityQualityIndex,
                        landuse,
                        buildingAge,
                        yearbuilt,
                        avgCrimeOut,
                        crimeCountOut,
                        dominantCrime,
                        crimeInspectionRisk
                };
                context.write(NullWritable.get(), new Text(toCsv(csv)));
            }
        }

        // Wczytuje wyniki joba statystycznego TEMP1 i przygotowuje mapy ZIP/BORO używane przy finalnym wzbogacaniu.
        private void loadTemp1Stats(Configuration conf) throws IOException {
            String statsPath = conf.get("temp2.temp1.stats.path");
            if (statsPath == null) {
                throw new IOException("Missing temp2.temp1.stats.path configuration");
            }

            Map<String, Double> boroNumberPerBoro = new HashMap<String, Double>();
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

                        if (k.startsWith("ZIP|")) {
                            String zip = k.substring("ZIP|".length());
                            String[] p = val.split("\\|", -1);
                            Double numberPerZip = p.length > 0 ? parseDouble(p[0]) : null;
                            Double avgScoreZip = p.length > 1 ? parseDouble(p[1]) : null;
                            if (numberPerZip != null) {
                                zipNumberPerZip.put(zip, numberPerZip);
                            }
                            if (avgScoreZip != null) {
                                zipAvgScore.put(zip, avgScoreZip);
                            }
                        } else if (k.startsWith("BORO|")) {
                            String boro = k.substring("BORO|".length());
                            Double numberPerBoro = parseDouble(val);
                            if (!isBlank(boro) && numberPerBoro != null) {
                                boroNumberPerBoro.put(boro, numberPerBoro);
                            }
                        }
                    }
                } finally {
                    br.close();
                }
            }

            stdevPerBoro = populationStdev(new ArrayList<Double>(boroNumberPerBoro.values()));
        }

        // Wczytuje agregaty NYPD po ZIP: count crime oraz dominant crime type.
        private void loadCrimeStats(Configuration conf) throws IOException {
            String crimePath = conf.get("temp2.crime.stats.path");
            if (crimePath == null) {
                throw new IOException("Missing temp2.crime.stats.path configuration");
            }

            FileSystem fs = FileSystem.get(conf);
            FileStatus[] files = fs.listStatus(new Path(crimePath));
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
                        String zip = line.substring(0, tab);
                        String val = line.substring(tab + 1);
                        String[] p = val.split("\\|", -1);
                        Integer count = p.length > 0 ? parseInt(p[0]) : null;
                        String dominant = p.length > 1 ? p[1] : "";
                        if (count != null) {
                            crimeCountByZip.put(zip, count);
                            if (!isBlank(dominant)) {
                                dominantCrimeByZip.put(zip, dominant);
                            }
                        }
                    }
                } finally {
                    br.close();
                }
            }
        }

        // Liczy globalne średnie i odchylenia dla NUMBER_PER_ZIP, NUMBER_PER_BORO, AVG_SCORE_ZIP oraz liczby przestępstw.
        private void calculateGlobalStatistics() {
            List<Double> numberPerZipValues = new ArrayList<Double>(zipNumberPerZip.values());
            stdevPerZip = populationStdev(numberPerZipValues);

            List<Double> avgScoreValues = new ArrayList<Double>(zipAvgScore.values());
            meanAvgScoreZip = mean(avgScoreValues);
            stdevAvgScoreZip = populationStdev(avgScoreValues);

            List<Double> crimeCountsAlignedToTemp1Zips = new ArrayList<Double>();
            for (String zip : zipNumberPerZip.keySet()) {
                crimeCountsAlignedToTemp1Zips.add(Double.valueOf(getCrimeCount(zip)));
            }
            meanCrimePerZip = mean(crimeCountsAlignedToTemp1Zips);
            stdevCrimePerZip = populationStdev(crimeCountsAlignedToTemp1Zips);
        }

        // Zwraca liczbę przestępstw dla ZIP; dla ZIP bez dopasowanych NYPD zwraca 0.
        private int getCrimeCount(String zip) {
            Integer count = crimeCountByZip.get(zip);
            return count == null ? 0 : count.intValue();
        }

        // Liczy RESTAURANT_DENSITY_QUALITY_INDEX = NUMBER_PER_ZIP / AVG_SCORE_ZIP; dla braku danych lub zera zwraca pustą wartość.
        private String calculateRestaurantDensityQualityIndex(String numberPerZip, String avgScoreZip) {
            Double n = parseDouble(numberPerZip);
            Double avg = parseDouble(avgScoreZip);
            if (n == null || avg == null || avg.doubleValue() == 0.0) {
                return "";
            }
            return formatDouble(n.doubleValue() / avg.doubleValue());
        }

        // Liczy BUILDING_AGE = currentYear - YEARBUILT tylko dla poprawnych lat z zakresu 1..currentYear.
        private String calculateBuildingAge(String yearbuilt, int currentYear) {
            Integer y = parseInt(onlyDigits(yearbuilt));
            if (y == null || y.intValue() <= 0 || y.intValue() > currentYear) {
                return "";
            }
            return String.valueOf(currentYear - y.intValue());
        }

        // Liczy pomocniczy risk score w TEMP2 jako suma z-score crime count i średniego score ZIP; finalnie jest przeliczany w TEMP3.
        private String calculateCrimeInspectionRisk(String zip, String avgScoreZip) {
            Double avgScore = parseDouble(avgScoreZip);
            if (avgScore == null) {
                return "";
            }

            double normCrime = stdevCrimePerZip == 0.0 ? 0.0 : (getCrimeCount(zip) - meanCrimePerZip) / stdevCrimePerZip;
            double normScore = stdevAvgScoreZip == 0.0 ? 0.0 : (avgScore.doubleValue() - meanAvgScoreZip) / stdevAvgScoreZip;
            return formatDouble(normCrime + normScore);
        }
    }

    // Zapisuje w konfiguracji indeksy kolumn TEMP1, akceptując nazwy z podkreśleniami i spacjami.
    private static void setTemp1Columns(Configuration conf, Map<String, Integer> h) {
        conf.setInt("temp1.idx.zipcode", firstExisting(h, 0, "ZIPCODE", "ZIP CODE"));
        conf.setInt("temp1.idx.camis", firstExisting(h, 1, "CAMIS"));
        conf.setInt("temp1.idx.score", firstExisting(h, 2, "SCORE"));
        conf.setInt("temp1.idx.cuisine", firstExisting(h, 3, "CUISINE_DESCRIPTION", "CUISINE DESCRIPTION"));
        conf.setInt("temp1.idx.address", firstExisting(h, 4, "ADDRESS"));
        conf.setInt("temp1.idx.boro", firstExisting(h, 5, "BORO", "BOROUGH"));
        conf.setInt("temp1.idx.number_per_zip", firstExisting(h, 6, "NUMBER_PER_ZIP", "NUMBER PER ZIP"));
        conf.setInt("temp1.idx.number_per_boro", firstExisting(h, 7, "NUMBER_PER_BORO", "NUMBER PER BORO"));
        conf.setInt("temp1.idx.avg_score_zip", firstExisting(h, 8, "AVG_SCORE_ZIP", "AVG SCORE ZIP"));
        conf.setInt("temp1.idx.avg_score_boro_cd", firstExisting(h, 9, "AVG_SCORE_BORO_CD", "AVG SCORE BORO CD"));
        conf.setInt("temp1.idx.landuse", firstExisting(h, 10, "LANDUSE", "LAND USE"));
        conf.setInt("temp1.idx.yearbuilt", firstExisting(h, 11, "YEARBUILT", "YEAR BUILT", "YEARBUIT"));
    }

    // Zapisuje w konfiguracji indeksy kolumn wzbogaconego NYPD_ZIP.
    private static void setNypdZipColumns(Configuration conf, Map<String, Integer> h) {
        conf.setInt("nypd.idx.cmplnt_num", firstExisting(h, 0, "CMPLNT_NUM", "COMPLAINT NUMBER"));
        conf.setInt("nypd.idx.zipcode", firstExisting(h, 1, "ZIPCODE", "ZIP CODE", "ZIP", "MODZCTA"));
        conf.setInt("nypd.idx.ofns_desc", firstExisting(h, 2, "OFNS_DESC", "OFFENSE DESCRIPTION"));
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

    // Łączy pola rekordów TEMP1 wewnętrznym separatorem US przed przekazaniem ich do reducera.
    private static String joinUS(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(US);
            sb.append(values[i] == null ? "" : values[i].replace(US, " "));
        }
        return sb.toString();
    }

    // Rozdziela wewnętrzny rekord TEMP2/TEMP1 zapisany separatorem US.
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

    // Pobiera wartość z mapy albo zwraca pusty string, jeśli klucz nie istnieje.
    private static String getOrEmpty(Map<String, String> map, String key) {
        String v = map.get(key);
        return v == null ? "" : v;
    }

    // Liczy średnią arytmetyczną z listy wartości liczbowych; dla pustej listy zwraca 0.0.
    private static double mean(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (Double v : values) {
            if (v != null) {
                sum += v.doubleValue();
            }
        }
        return sum / values.size();
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
        for (Double v : values) {
            double d = v.doubleValue() - avg;
            sumSq += d * d;
        }
        return Math.sqrt(sumSq / values.size());
    }

    // Formatuje Double do czterech miejsc po przecinku albo zwraca pustą wartość dla null.
    private static String formatNullableDouble(Double v) {
        return v == null ? "" : formatDouble(v.doubleValue());
    }

    // Formatuje liczbę zmiennoprzecinkową do czterech miejsc po przecinku z separatorem kropki.
    private static String formatDouble(double v) {
        return String.format(Locale.US, "%.4f", v);
    }

    // Wybiera najczęstszy typ przestępstwa; przy remisie wygrywa alfabetycznie pierwszy typ.
    private static String chooseDominantAlphabetically(Map<String, Integer> counts) {
        if (counts == null || counts.isEmpty()) {
            return "";
        }
        List<String> offenses = new ArrayList<String>(counts.keySet());
        Collections.sort(offenses);
        String best = "";
        int bestCount = -1;
        for (String offense : offenses) {
            int count = counts.get(offense).intValue();
            if (count > bestCount) {
                best = offense;
                bestCount = count;
            }
        }
        return best;
    }

    // Zwraca bieżący rok systemowy używany do obliczenia wieku budynku.
    private static int getCurrentYear() {
        return Integer.parseInt(new SimpleDateFormat("yyyy").format(new Date()));
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

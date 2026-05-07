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

/**
 * NYPD -> ZIP/MODZCTA enrichment MapReduce job.
 *
 * Purpose:
 *   NYPD Complaint Data Historic does not contain a ZIPCODE column.
 *   This job assigns each NYPD complaint to a ZIP/MODZCTA area by checking
 *   whether the complaint point (Longitude, Latitude) falls inside a MODZCTA polygon.
 *
 * Default input:
 *   /datasets/static/NYPD.csv
 *   /datasets/static/MODZCTA.csv
 *
 * Default output:
 *   /datasets/results/nypd_zip_yyyyMMdd_HHmmss.csv
 *   /datasets/results/nypd_zip_latest.csv
 *
 * Output schema:
 *   CMPLNT_NUM,ZIPCODE,OFNS_DESC,BORO_NM,LATITUDE,LONGITUDE
 *
 * Hadoop: 3.3.5
 * Java:   8
 * External libraries: none
 */
public class NypdZipMapReduce extends Configured implements Tool {

    private static final String HEADER = "CMPLNT_NUM,ZIPCODE,OFNS_DESC,BORO_NM,LATITUDE,LONGITUDE";

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new NypdZipMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        String nypdInput = args.length > 0 ? args[0] : "/datasets/static/NYPD.csv";
        String modzctaInput = args.length > 1 ? args[1] : "/datasets/static/MODZCTA.csv";
        String resultsDir = args.length > 2 ? args[2] : "/datasets/results";

        Configuration baseConf = getConf();
        FileSystem fs = FileSystem.get(baseConf);
        fs.mkdirs(new Path(resultsDir));

        String ts = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        Path workDir = new Path(resultsDir + "/_nypd_zip_work_" + ts);
        Path jobOut = new Path(workDir, "nypd_zip_dir");
        Path versionedFile = new Path(resultsDir + "/nypd_zip_" + ts + ".csv");
        Path latestFile = new Path(resultsDir + "/nypd_zip_latest.csv");

        if (fs.exists(workDir)) {
            fs.delete(workDir, true);
        }

        Map<String, Integer> nypdHeader = readHeaderIndex(baseConf, new Path(nypdInput));

        Configuration jobConf = new Configuration(baseConf);
        jobConf.set("nypdzip.modzcta.path", modzctaInput);
        setNypdColumns(jobConf, nypdHeader);

        Job job = Job.getInstance(jobConf, "NYPD ZIP enrichment - point in MODZCTA polygon");
        job.setJarByClass(NypdZipMapReduce.class);
        job.setMapperClass(NypdZipMapper.class);
        job.setReducerClass(SingleCsvReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1); // one CSV file and one header

        FileInputFormat.addInputPath(job, new Path(nypdInput));
        FileOutputFormat.setOutputPath(job, jobOut);

        if (!job.waitForCompletion(true)) {
            return 1;
        }

        Path partFile = findFirstPartFile(fs, jobOut);
        if (partFile == null) {
            throw new IOException("No part file found in " + jobOut);
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

        System.out.println("NYPD ZIP versioned output: " + versionedFile);
        System.out.println("NYPD ZIP latest output:    " + latestFile);
        System.out.println("Requested HDFS replication factor: 3");
        return 0;
    }

    public static class NypdZipMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private int idxCmplntNum;
        private int idxOfnsDesc;
        private int idxBoroNm;
        private int idxLatitude;
        private int idxLongitude;
        private int maxIdx;
        private List<ZipArea> zipAreas;

        enum Counters {
            MODZCTA_AREAS_LOADED,
            MODZCTA_AREAS_WITHOUT_GEOMETRY,
            NYPD_HEADER_ROWS,
            NYPD_SHORT_ROWS,
            NYPD_MISSING_REQUIRED_FIELDS,
            NYPD_INVALID_COORDINATES,
            NYPD_OUTSIDE_MODZCTA,
            NYPD_ROWS_WITH_ZIP
        }

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            idxCmplntNum = conf.getInt("nypd.idx.cmplnt_num", 0);
            idxBoroNm = conf.getInt("nypd.idx.boro_nm", 2);
            idxOfnsDesc = conf.getInt("nypd.idx.ofns_desc", 15);
            idxLatitude = conf.getInt("nypd.idx.latitude", 32);
            idxLongitude = conf.getInt("nypd.idx.longitude", 33);
            maxIdx = max(idxCmplntNum, idxBoroNm, idxOfnsDesc, idxLatitude, idxLongitude);

            String modzctaPath = conf.get("nypdzip.modzcta.path");
            if (isBlank(modzctaPath)) {
                throw new IOException("Missing nypdzip.modzcta.path configuration");
            }
            zipAreas = loadZipAreas(conf, new Path(modzctaPath), context);
            if (zipAreas.isEmpty()) {
                throw new IOException("No MODZCTA polygons loaded from " + modzctaPath);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) {
                return;
            }

            List<String> f = parseCsvLine(line);
            if (isCsvHeader(f, "CMPLNT_NUM")) {
                context.getCounter(Counters.NYPD_HEADER_ROWS).increment(1L);
                return;
            }
            if (f.size() <= maxIdx) {
                context.getCounter(Counters.NYPD_SHORT_ROWS).increment(1L);
                return;
            }

            String cmplntNum = clean(get(f, idxCmplntNum));
            String ofnsDesc = upper(get(f, idxOfnsDesc));
            String boroNm = upper(get(f, idxBoroNm));
            Double lat = parseDouble(clean(get(f, idxLatitude)));
            Double lon = parseDouble(clean(get(f, idxLongitude)));

            if (isBlank(cmplntNum) || isBlank(ofnsDesc)) {
                context.getCounter(Counters.NYPD_MISSING_REQUIRED_FIELDS).increment(1L);
                return;
            }
            if (lat == null || lon == null || !isPlausibleNycCoordinate(lat.doubleValue(), lon.doubleValue())) {
                context.getCounter(Counters.NYPD_INVALID_COORDINATES).increment(1L);
                return;
            }

            String zip = findZip(lat.doubleValue(), lon.doubleValue(), zipAreas);
            if (isBlank(zip)) {
                context.getCounter(Counters.NYPD_OUTSIDE_MODZCTA).increment(1L);
                return;
            }

            String[] out = new String[] {
                    cmplntNum,
                    zip,
                    ofnsDesc,
                    boroNm,
                    formatCoordinate(lat.doubleValue()),
                    formatCoordinate(lon.doubleValue())
            };
            context.write(NullWritable.get(), new Text(toCsv(out)));
            context.getCounter(Counters.NYPD_ROWS_WITH_ZIP).increment(1L);
        }
    }

    public static class SingleCsvReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), new Text(HEADER));
        }

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                context.write(NullWritable.get(), v);
            }
        }
    }

    private static List<ZipArea> loadZipAreas(Configuration conf, Path modzctaPath, Mapper.Context context) throws IOException {
        List<ZipArea> out = new ArrayList<ZipArea>();
        FileSystem fs = modzctaPath.getFileSystem(conf);
        FSDataInputStream in = fs.open(modzctaPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        try {
            String headerLine = br.readLine();
            if (headerLine == null) {
                return out;
            }

            List<String> headers = parseCsvLine(headerLine);
            Map<String, Integer> h = new HashMap<String, Integer>();
            for (int i = 0; i < headers.size(); i++) {
                h.put(canon(headers.get(i)), i);
            }

            int idxZip = firstExisting(h, -1, "MODZCTA", "ZCTA", "ZIPCODE", "ZIP CODE", "POSTCODE", "ZIP");
            int idxGeom = firstExisting(h, -1, "THE_GEOM", "THE GEOM", "GEOMETRY", "GEOM", "MULTIPOLYGON", "POLYGON", "SHAPE", "WKT", "NEW GEOREFERENCED COLUMN", "GEOCODED_COLUMN", "GEOCODED COLUMN");

            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                List<String> f = parseCsvLine(line);
                String zip = idxZip >= 0 ? firstFiveDigits(clean(get(f, idxZip))) : findFirstZipLikeField(f);
                String geometry = idxGeom >= 0 ? clean(get(f, idxGeom)) : findFirstGeometryField(f);

                if (isBlank(zip)) {
                    continue;
                }
                if (isBlank(geometry)) {
                    if (context != null) {
                        context.getCounter(NypdZipMapper.Counters.MODZCTA_AREAS_WITHOUT_GEOMETRY).increment(1L);
                    }
                    continue;
                }

                List<Polygon> polygons = parseGeometry(geometry);
                if (!polygons.isEmpty()) {
                    out.add(new ZipArea(zip, polygons));
                    if (context != null) {
                        context.getCounter(NypdZipMapper.Counters.MODZCTA_AREAS_LOADED).increment(1L);
                    }
                } else if (context != null) {
                    context.getCounter(NypdZipMapper.Counters.MODZCTA_AREAS_WITHOUT_GEOMETRY).increment(1L);
                }
            }
        } finally {
            br.close();
        }
        return out;
    }

    private static String findZip(double lat, double lon, List<ZipArea> areas) {
        for (ZipArea area : areas) {
            if (area.contains(lat, lon)) {
                return area.zip;
            }
        }
        return "";
    }

    private static List<Polygon> parseGeometry(String geometry) {
        String g = clean(geometry);
        if (g.length() == 0) {
            return new ArrayList<Polygon>();
        }
        String u = g.toUpperCase(Locale.ROOT);
        if (u.startsWith("MULTIPOLYGON") || u.startsWith("POLYGON")) {
            return parseWktGeometry(g);
        }
        if (u.indexOf("COORDINATES") >= 0 && (u.indexOf("MULTIPOLYGON") >= 0 || u.indexOf("POLYGON") >= 0)) {
            return parseGeoJsonGeometry(g);
        }
        return new ArrayList<Polygon>();
    }

    private static List<Polygon> parseWktGeometry(String wkt) {
        List<Polygon> polygons = new ArrayList<Polygon>();
        String u = wkt.toUpperCase(Locale.ROOT).trim();
        if (u.startsWith("POLYGON")) {
            String body = stripOuterParens(wkt.substring(wkt.indexOf('(')).trim());
            List<String> ringGroups = directParenGroups(body);
            Polygon p = polygonFromWktRingGroups(ringGroups);
            if (p != null && !p.rings.isEmpty()) {
                polygons.add(p);
            }
        } else if (u.startsWith("MULTIPOLYGON")) {
            String body = stripOuterParens(wkt.substring(wkt.indexOf('(')).trim());
            List<String> polygonGroups = directParenGroups(body);
            for (String pg : polygonGroups) {
                String polygonBody = stripOuterParens(pg);
                List<String> ringGroups = directParenGroups(polygonBody);
                Polygon p = polygonFromWktRingGroups(ringGroups);
                if (p != null && !p.rings.isEmpty()) {
                    polygons.add(p);
                }
            }
        }
        return polygons;
    }

    private static Polygon polygonFromWktRingGroups(List<String> ringGroups) {
        List<Ring> rings = new ArrayList<Ring>();
        for (String rg : ringGroups) {
            String ringBody = stripOuterParens(rg);
            Ring ring = parseWktRing(ringBody);
            if (ring.points.size() >= 3) {
                rings.add(ring);
            }
        }
        return rings.isEmpty() ? null : new Polygon(rings);
    }

    private static Ring parseWktRing(String ringBody) {
        List<Point> points = new ArrayList<Point>();
        String[] pairs = ringBody.split(",");
        for (String pair : pairs) {
            String[] nums = pair.trim().split("\\s+");
            if (nums.length >= 2) {
                Double lon = parseDouble(nums[0]);
                Double lat = parseDouble(nums[1]);
                if (lon != null && lat != null) {
                    points.add(new Point(lat.doubleValue(), lon.doubleValue()));
                }
            }
        }
        return new Ring(points);
    }

    private static List<Polygon> parseGeoJsonGeometry(String json) {
        List<Polygon> polygons = new ArrayList<Polygon>();
        String upper = json.toUpperCase(Locale.ROOT);
        int coordIdx = upper.indexOf("COORDINATES");
        if (coordIdx < 0) {
            return polygons;
        }
        int firstBracket = json.indexOf('[', coordIdx);
        int lastBracket = json.lastIndexOf(']');
        if (firstBracket < 0 || lastBracket <= firstBracket) {
            return polygons;
        }
        String coords = json.substring(firstBracket, lastBracket + 1);

        boolean isMulti = upper.indexOf("MULTIPOLYGON") >= 0;
        if (isMulti) {
            String body = stripOuterSquare(coords);
            List<String> polygonGroups = directSquareGroups(body);
            for (String pg : polygonGroups) {
                String polygonBody = stripOuterSquare(pg);
                List<String> ringGroups = directSquareGroups(polygonBody);
                Polygon p = polygonFromGeoJsonRingGroups(ringGroups);
                if (p != null && !p.rings.isEmpty()) {
                    polygons.add(p);
                }
            }
        } else {
            String body = stripOuterSquare(coords);
            List<String> ringGroups = directSquareGroups(body);
            Polygon p = polygonFromGeoJsonRingGroups(ringGroups);
            if (p != null && !p.rings.isEmpty()) {
                polygons.add(p);
            }
        }
        return polygons;
    }

    private static Polygon polygonFromGeoJsonRingGroups(List<String> ringGroups) {
        List<Ring> rings = new ArrayList<Ring>();
        for (String rg : ringGroups) {
            Ring ring = parseGeoJsonRing(rg);
            if (ring.points.size() >= 3) {
                rings.add(ring);
            }
        }
        return rings.isEmpty() ? null : new Polygon(rings);
    }

    private static Ring parseGeoJsonRing(String ringJson) {
        List<Point> points = new ArrayList<Point>();
        String body = stripOuterSquare(ringJson);
        List<String> pairGroups = directSquareGroups(body);
        for (String pairGroup : pairGroups) {
            String pairBody = stripOuterSquare(pairGroup).trim();
            String[] nums = pairBody.split(",");
            if (nums.length >= 2) {
                Double lon = parseDouble(nums[0]);
                Double lat = parseDouble(nums[1]);
                if (lon != null && lat != null) {
                    points.add(new Point(lat.doubleValue(), lon.doubleValue()));
                }
            }
        }
        return new Ring(points);
    }

    private static List<String> directParenGroups(String s) {
        List<String> groups = new ArrayList<String>();
        int depth = 0;
        int start = -1;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') {
                if (depth == 0) {
                    start = i;
                }
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0 && start >= 0) {
                    groups.add(s.substring(start, i + 1));
                    start = -1;
                }
            }
        }
        return groups;
    }

    private static List<String> directSquareGroups(String s) {
        List<String> groups = new ArrayList<String>();
        int depth = 0;
        int start = -1;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '[') {
                if (depth == 0) {
                    start = i;
                }
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0 && start >= 0) {
                    groups.add(s.substring(start, i + 1));
                    start = -1;
                }
            }
        }
        return groups;
    }

    private static String stripOuterParens(String s) {
        String x = clean(s);
        if (x.startsWith("(") && x.endsWith(")") && enclosesWholeExpression(x, '(', ')')) {
            return x.substring(1, x.length() - 1).trim();
        }
        return x;
    }

    private static String stripOuterSquare(String s) {
        String x = clean(s);
        if (x.startsWith("[") && x.endsWith("]") && enclosesWholeExpression(x, '[', ']')) {
            return x.substring(1, x.length() - 1).trim();
        }
        return x;
    }

    private static boolean enclosesWholeExpression(String s, char open, char close) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == open) {
                depth++;
            } else if (c == close) {
                depth--;
                if (depth == 0 && i < s.length() - 1) {
                    return false;
                }
            }
        }
        return depth == 0;
    }

    private static class Point {
        final double lat;
        final double lon;
        Point(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }

    private static class Ring {
        final List<Point> points;
        final double minLat;
        final double maxLat;
        final double minLon;
        final double maxLon;

        Ring(List<Point> points) {
            this.points = points;
            double a = Double.POSITIVE_INFINITY;
            double b = Double.NEGATIVE_INFINITY;
            double c = Double.POSITIVE_INFINITY;
            double d = Double.NEGATIVE_INFINITY;
            for (Point p : points) {
                if (p.lat < a) a = p.lat;
                if (p.lat > b) b = p.lat;
                if (p.lon < c) c = p.lon;
                if (p.lon > d) d = p.lon;
            }
            this.minLat = a;
            this.maxLat = b;
            this.minLon = c;
            this.maxLon = d;
        }

        boolean mayContain(double lat, double lon) {
            return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
        }

        boolean contains(double lat, double lon) {
            if (!mayContain(lat, lon)) {
                return false;
            }
            boolean inside = false;
            int n = points.size();
            for (int i = 0, j = n - 1; i < n; j = i++) {
                Point pi = points.get(i);
                Point pj = points.get(j);
                boolean intersect = ((pi.lat > lat) != (pj.lat > lat))
                        && (lon < (pj.lon - pi.lon) * (lat - pi.lat) / ((pj.lat - pi.lat) + 0.0) + pi.lon);
                if (intersect) {
                    inside = !inside;
                }
            }
            return inside;
        }
    }

    private static class Polygon {
        final List<Ring> rings;
        final double minLat;
        final double maxLat;
        final double minLon;
        final double maxLon;

        Polygon(List<Ring> rings) {
            this.rings = rings;
            double a = Double.POSITIVE_INFINITY;
            double b = Double.NEGATIVE_INFINITY;
            double c = Double.POSITIVE_INFINITY;
            double d = Double.NEGATIVE_INFINITY;
            for (Ring r : rings) {
                if (r.minLat < a) a = r.minLat;
                if (r.maxLat > b) b = r.maxLat;
                if (r.minLon < c) c = r.minLon;
                if (r.maxLon > d) d = r.maxLon;
            }
            this.minLat = a;
            this.maxLat = b;
            this.minLon = c;
            this.maxLon = d;
        }

        boolean mayContain(double lat, double lon) {
            return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
        }

        boolean contains(double lat, double lon) {
            if (!mayContain(lat, lon) || rings.isEmpty()) {
                return false;
            }
            // First ring is treated as the outer boundary. Additional rings are holes.
            if (!rings.get(0).contains(lat, lon)) {
                return false;
            }
            for (int i = 1; i < rings.size(); i++) {
                if (rings.get(i).contains(lat, lon)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class ZipArea {
        final String zip;
        final List<Polygon> polygons;
        final double minLat;
        final double maxLat;
        final double minLon;
        final double maxLon;

        ZipArea(String zip, List<Polygon> polygons) {
            this.zip = zip;
            this.polygons = polygons;
            double a = Double.POSITIVE_INFINITY;
            double b = Double.NEGATIVE_INFINITY;
            double c = Double.POSITIVE_INFINITY;
            double d = Double.NEGATIVE_INFINITY;
            for (Polygon p : polygons) {
                if (p.minLat < a) a = p.minLat;
                if (p.maxLat > b) b = p.maxLat;
                if (p.minLon < c) c = p.minLon;
                if (p.maxLon > d) d = p.maxLon;
            }
            this.minLat = a;
            this.maxLat = b;
            this.minLon = c;
            this.maxLon = d;
        }

        boolean mayContain(double lat, double lon) {
            return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
        }

        boolean contains(double lat, double lon) {
            if (!mayContain(lat, lon)) {
                return false;
            }
            for (Polygon p : polygons) {
                if (p.contains(lat, lon)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static void setNypdColumns(Configuration conf, Map<String, Integer> h) {
        conf.setInt("nypd.idx.cmplnt_num", firstExisting(h, 0, "CMPLNT_NUM", "CMPLNT NUM"));
        conf.setInt("nypd.idx.boro_nm", firstExisting(h, 2, "BORO_NM", "BORO NM", "BOROUGH", "BORO"));
        conf.setInt("nypd.idx.ofns_desc", firstExisting(h, 15, "OFNS_DESC", "OFNS DESC", "OFFENSE DESCRIPTION"));
        conf.setInt("nypd.idx.latitude", firstExisting(h, 32, "LATITUDE", "LAT"));
        conf.setInt("nypd.idx.longitude", firstExisting(h, 33, "LONGITUDE", "LON", "LNG"));
    }

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

    private static int firstExisting(Map<String, Integer> h, int defaultIndex, String... names) {
        for (String n : names) {
            Integer idx = h.get(canon(n));
            if (idx != null) {
                return idx.intValue();
            }
        }
        return defaultIndex;
    }

    private static String findFirstZipLikeField(List<String> fields) {
        for (String f : fields) {
            String d = firstFiveDigits(clean(f));
            if (d.length() == 5) {
                return d;
            }
        }
        return "";
    }

    private static String findFirstGeometryField(List<String> fields) {
        for (String f : fields) {
            String u = clean(f).toUpperCase(Locale.ROOT);
            if (u.startsWith("POLYGON") || u.startsWith("MULTIPOLYGON") || u.indexOf("COORDINATES") >= 0) {
                return f;
            }
        }
        return "";
    }

    private static Path findFirstPartFile(FileSystem fs, Path dir) throws IOException {
        FileStatus[] statuses = fs.listStatus(dir);
        for (FileStatus st : statuses) {
            if (st.isFile() && st.getPath().getName().startsWith("part-")) {
                return st.getPath();
            }
        }
        return null;
    }

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

    private static boolean isCsvHeader(List<String> f, String expectedFirstColumn) {
        if (f == null || f.isEmpty()) {
            return false;
        }
        return canon(f.get(0)).equals(canon(expectedFirstColumn));
    }

    private static String get(List<String> fields, int idx) {
        return idx >= 0 && idx < fields.size() ? fields.get(idx) : "";
    }

    private static String clean(String s) {
        if (s == null) {
            return "";
        }
        return s.trim().replace('\uFEFF', ' ');
    }

    private static String upper(String s) {
        return clean(s).toUpperCase(Locale.ROOT);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

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

    private static String firstFiveDigits(String s) {
        String d = onlyDigits(s);
        return d.length() >= 5 ? d.substring(0, 5) : d;
    }

    private static Double parseDouble(String s) {
        try {
            if (s == null || s.trim().isEmpty()) {
                return null;
            }
            String x = s.trim();
            if (x.indexOf(' ') >= 0) {
                x = x.split("\\s+")[0];
            }
            return Double.valueOf(x);
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isPlausibleNycCoordinate(double lat, double lon) {
        // Broad NYC bounding box with small buffer.
        return lat >= 40.30 && lat <= 41.10 && lon >= -74.60 && lon <= -73.40;
    }

    private static String toCsv(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(csvEscape(values[i]));
        }
        return sb.toString();
    }

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

    private static String canon(String s) {
        if (s == null) return "";
        return s.trim().toUpperCase(Locale.ROOT).replace('_', ' ').replaceAll("\\s+", " ");
    }

    private static String formatCoordinate(double v) {
        return String.format(Locale.US, "%.8f", v);
    }

    private static int max(int... values) {
        int m = Integer.MIN_VALUE;
        for (int v : values) {
            if (v > m) m = v;
        }
        return m;
    }
}

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * StudentPerformanceAnalysis (dist.java)
 *
 * Hadoop MapReduce program using Distributed Cache to perform a
 * Map-Side Join between:
 *   - s.csv  (student info: Regno, Name, Course)   ← cached in Distributed Cache
 *   - m.csv  (marks:        Regno, Sub1, Sub2, Sub3) ← primary HDFS input
 *
 * For each student, the program joins their name/course with their
 * marks and determines Pass/Fail based on threshold (≥ 50 in all 3 subjects).
 *
 * Real-World Use Case:
 *   Universities processing millions of student exam results across
 *   distributed nodes, producing result cards without a centralised DB.
 */
public class dist {

    // Pass threshold (marks out of 100)
    private static final int PASS_THRESHOLD = 50;

    // ─────────────────────────────────────────────────────────────
    //  MAPPER — performs the Map-Side Join using Distributed Cache
    //
    //  setup()  : Loads s.csv from Distributed Cache into a HashMap
    //  map()    : For each marks record, looks up student info and
    //             emits (Regno, "Name,Course,Sub1,Sub2,Sub3")
    // ─────────────────────────────────────────────────────────────
    public static class DistMapper extends Mapper<LongWritable, Text, Text, Text> {

        // In-memory lookup table: Regno → "Name,Course"
        private final Map<String, String> studentCache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load the cached student info file into memory
            // Hadoop makes the file available at ./s.csv on each node
            File cachedFile = new File("./s.csv");
            try (BufferedReader reader = new BufferedReader(new FileReader(cachedFile))) {
                String line;
                boolean isHeader = true;
                while ((line = reader.readLine()) != null) {
                    if (isHeader) { isHeader = false; continue; }  // skip header

                    String[] parts = line.split(",", 3);
                    if (parts.length < 3) continue;

                    String regno = parts[0].trim();
                    String name  = parts[1].trim();
                    String course = parts[2].trim();

                    studentCache.put(regno, name + "," + course);
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.startsWith("Regno") || line.isEmpty()) return;  // skip header

            String[] parts = line.split(",");
            if (parts.length < 4) return;

            String regno = parts[0].trim();
            String sub1  = parts[1].trim();
            String sub2  = parts[2].trim();
            String sub3  = parts[3].trim();

            // Lookup in cache (Map-Side Join — no shuffle needed for student info)
            String studentInfo = studentCache.get(regno);
            if (studentInfo == null) return;  // no matching student record

            // Emit: key=Regno, value="Name,Course,Sub1,Sub2,Sub3"
            String outValue = studentInfo + "," + sub1 + "," + sub2 + "," + sub3;
            context.write(new Text(regno), new Text(outValue));
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  REDUCER — determines Pass/Fail for each student
    //
    //  Input:  (Regno, ["Name,Course,Sub1,Sub2,Sub3"])
    //  Output: (Regno, "Name,Pass/Fail")
    // ─────────────────────────────────────────────────────────────
    public static class DistReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                // Format: "Name,Course,Sub1,Sub2,Sub3"
                String[] parts = val.toString().split(",", 5);
                if (parts.length < 5) continue;

                String name = parts[0].trim();
                // parts[1] = course (not used in output but available)

                try {
                    int sub1 = Integer.parseInt(parts[2].trim());
                    int sub2 = Integer.parseInt(parts[3].trim());
                    int sub3 = Integer.parseInt(parts[4].trim());

                    // Pass only if all three subjects meet threshold
                    String result = (sub1 >= PASS_THRESHOLD &&
                                     sub2 >= PASS_THRESHOLD &&
                                     sub3 >= PASS_THRESHOLD) ? "Pass" : "Fail";

                    context.write(key, new Text(name + "," + result));

                } catch (NumberFormatException e) {
                    // Skip malformed marks records
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  DRIVER
    // ─────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: dist <marks-input-path> <output-path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Student Performance Distributed Cache Analysis");

        job.setJarByClass(dist.class);

        // Add s.csv to Distributed Cache — aliased as "s.csv" on each node
        // The file must already exist in HDFS at this path
        job.addCacheFile(new URI("hdfs:///practice/DClass/s.csv#s.csv"));

        job.setMapperClass(DistMapper.class);
        job.setReducerClass(DistReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}

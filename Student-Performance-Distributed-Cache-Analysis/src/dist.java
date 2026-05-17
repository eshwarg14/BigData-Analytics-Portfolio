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

public class dist {

    private static final int PASS_THRESHOLD = 50;

    public static class DistMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<String, String> studentCache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            File cachedFile = new File("./s.csv");

            try (BufferedReader reader = new BufferedReader(new FileReader(cachedFile))) {
                String line;
                boolean isHeader = true;

                while ((line = reader.readLine()) != null) {

                    if (isHeader) {
                        isHeader = false;
                        continue;
                    }

                    String[] parts = line.split(",", 3);
                    if (parts.length < 3) continue;

                    String regno = parts[0].trim();
                    String name = parts[1].trim();
                    String course = parts[2].trim();

                    studentCache.put(regno, name + "," + course);
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.startsWith("Regno") || line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 4) return;

            String regno = parts[0].trim();
            String sub1 = parts[1].trim();
            String sub2 = parts[2].trim();
            String sub3 = parts[3].trim();

            String studentInfo = studentCache.get(regno);

            if (studentInfo == null) return;

            String outValue = studentInfo + "," + sub1 + "," + sub2 + "," + sub3;

            context.write(new Text(regno), new Text(outValue));
        }
    }

    public static class DistReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {

                String[] parts = val.toString().split(",", 5);

                if (parts.length < 5) continue;

                String name = parts[0].trim();

                try {
                    int sub1 = Integer.parseInt(parts[2].trim());
                    int sub2 = Integer.parseInt(parts[3].trim());
                    int sub3 = Integer.parseInt(parts[4].trim());

                    String result = (sub1 >= PASS_THRESHOLD &&
                                     sub2 >= PASS_THRESHOLD &&
                                     sub3 >= PASS_THRESHOLD) ? "Pass" : "Fail";

                    context.write(key, new Text(name + "," + result));

                } catch (NumberFormatException e) {
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: dist <marks-input-path> <output-path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Student Performance Distributed Cache Analysis");

        job.setJarByClass(dist.class);

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

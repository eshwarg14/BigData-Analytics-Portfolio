import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReviewPartitioner {

    public static class ReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text compoundKey = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] cols = value.toString().toLowerCase().split(",");

            if (cols.length >= 2) {
                String product = cols[0].trim();
                String review = cols[1].trim();

                if (review.contains("damaged")) {
                    compoundKey.set(product + "_damaged");
                    context.write(compoundKey, one);
                } else if (review.contains("good")) {
                    compoundKey.set(product + "_good");
                    context.write(compoundKey, one);
                }
            }
        }
    }

    public static class ReviewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SentimentPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            if (numReduceTasks == 0) return 0;
            return key.toString().endsWith("_damaged") ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: ReviewPartitioner <input-path> <output-path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Sentiment Partition Analysis");

        job.setJarByClass(ReviewPartitioner.class);

        job.setMapperClass(ReviewMapper.class);
        job.setReducerClass(ReviewReducer.class);
        job.setPartitionerClass(SentimentPartitioner.class);

        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

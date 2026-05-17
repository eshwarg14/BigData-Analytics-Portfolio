import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * DamagedProductReviewAnalysis
 *
 * A Hadoop MapReduce job that reads e-commerce product reviews (CSV format)
 * and counts how many times the word "damaged" appears per product.
 *
 * Input format:  ProductName,Review Text
 * Output format: ProductName   <count_of_"damaged"_occurrences>
 *
 * Dataset: 1000 Flipkart/Amazon electronic product reviews (Kaggle)
 */
public class wordcount {

    // ─────────────────────────────────────────────────────────────
    //  MAPPER
    //  Input : <byte-offset, CSV line>
    //  Output: <ProductName, 1>  — emitted only when review contains "damaged"
    // ─────────────────────────────────────────────────────────────
    public static class wordmapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text productKey = new Text();
        private final IntWritable one  = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Each line: "ProductName,Review text possibly with spaces"
            String[] columns = value.toString().toLowerCase().split(",");

            if (columns.length >= 2) {
                String product = columns[0].trim();
                String review  = columns[1].trim();

                // Tokenise the review on whitespace
                String[] words = review.split("\\s+");

                for (String word : words) {
                    if (word.equals("damaged")) {
                        productKey.set(product);
                        context.write(productKey, one);
                    }
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  REDUCER
    //  Input : <ProductName, [1, 1, 1, ...]>
    //  Output: <ProductName, total_damaged_count>
    // ─────────────────────────────────────────────────────────────
    public static class wordreducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int total = 0;
            for (IntWritable val : values) {
                total += val.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  DRIVER
    // ─────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: wordcount <input-path> <output-path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Damaged Product Review Analysis");

        job.setJarByClass(wordcount.class);

        // Formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Map & Reduce
        job.setMapperClass(wordmapper.class);
        job.setReducerClass(wordreducer.class);
        job.setNumReduceTasks(1);

        // Paths
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success ? "Job completed successfully." : "Job failed.");
        System.exit(success ? 0 : 1);
    }
}

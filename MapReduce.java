import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapReduce {
    public static enum Global_Counters {
        REVIEWS_COUNT
    }

    // Mapper for word count
    public static class cer extends Mapper<Object, Text, Text, IntWritable> {
        private Text wordReviewKey = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(Global_Counters.REVIEWS_COUNT).increment(1);
            String line = value.toString();
            String[] columns = line.split(",");

            if (columns.length < 3) return;

            String reviewId = columns[0];
            String reviewText = columns[2];
            for (String word : reviewText.split(" ")) {
                if (!word.isEmpty()) {
                    wordReviewKey.set(word + "@" + reviewId);
                    context.write(wordReviewKey, one);
                }
            }
        }
    }

    // Reducer for word count
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Mapper for Term Frequency (TF)
    public static class TFMap extends Mapper<Object, Text, Text, Text> {
        private Text reviewKey = new Text();
        private Text wordCountValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\t");
            String[] splittedKey = lines[0].split("@");

            reviewKey.set(splittedKey[1]);
            wordCountValue.set(splittedKey[0] + "$" + lines[1]);

            context.write(reviewKey, wordCountValue);
        }
    }

    // Reducer for Term Frequency (TF)
    public static class TFReduce extends Reducer<Text, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        HashMap<String, Integer> wordCounts = new HashMap<>();

        for (Text value : values) {
            String[] wordAndCount = value.toString().split("\\$");
            if (wordAndCount.length < 2) continue;
            
            String word = wordAndCount[0];
            int count = Integer.parseInt(wordAndCount[1]);
            wordCounts.put(word, count);
            total += count;
        }

        for (Map.Entry<String, Integer> mpEntry : wordCounts.entrySet()) {
            outputKey.set(mpEntry.getKey() + "@" + key.toString());
            outputValue.set(mpEntry.getValue() + "&" + total);
            context.write(outputKey, outputValue);
        }
    }
}

    // Mapper for TF-IDF
    public static class TFIDFMap extends Mapper<Object, Text, Text, Text> {
        private Text wordKey = new Text();
        private Text reviewCountValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");
            String[] splittedKey = columns[0].split("@");

            wordKey.set(splittedKey[0]);
            reviewCountValue.set(splittedKey[1] + "$" + columns[1]);

            context.write(wordKey, reviewCountValue);
        }
    }

    // Reducer for TF-IDF
    public static class TFIDFReduce extends Reducer<Text, Text, Text, Text> {
    private static int numOfReviews;
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        numOfReviews = Integer.parseInt(conf.get("reviews_count"));
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Double> reviewTFValues = new HashMap<>();
        int numOfReviewsWithThisWord = 0;

        // Process each value to calculate TF
        for (Text value : values) {
            String[] valueParts = value.toString().split("\\$");
            if (valueParts.length < 2) continue; 

            String word = valueParts[0];
            String[] tfParts = valueParts[1].split("&");
            double wordCount = Double.parseDouble(tfParts[0]);
            double reviewLength = Double.parseDouble(tfParts[1]);
            double tf = wordCount / reviewLength;

            reviewTFValues.put(word, tf);
            numOfReviewsWithThisWord++;
        }

        // Calculate IDF
        double idf = Math.log((double) numOfReviews / numOfReviewsWithThisWord);

        // Calculate and write TF-IDF values
        for (Map.Entry<String, Double> entry : reviewTFValues.entrySet()) {
            double tfidf = entry.getValue() * idf;
            outputKey.set(entry.getKey() + "@" + key.toString());
            outputValue.set(String.valueOf(tfidf));
            context.write(outputKey, outputValue);
        }
    }
}

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Define output directories
        Path wordCountDir = new Path(outputPath, "wordcount");
        Path tfDir = new Path(outputPath, "tf");
        Path tfidfDir = new Path(outputPath, "tfidf");

        // Word Count Job
        Job wordCountJob = Job.getInstance(conf, "word count");
        wordCountJob.setJarByClass(Stats.class);
        wordCountJob.setMapperClass(TokenizerMapper.class);
        wordCountJob.setCombinerClass(IntSumReducer.class);
        wordCountJob.setReducerClass(IntSumReducer.class);
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(wordCountJob, inputPath);
        TextOutputFormat.setOutputPath(wordCountJob, wordCountDir);
        wordCountJob.waitForCompletion(true);

        // Count total number of reviews
        String numOfReviewsStr = String.valueOf(wordCountJob.getCounters().findCounter(Global_Counters.REVIEWS_COUNT).getValue());
        conf.set("reviews_count", numOfReviewsStr);

        // Term Frequency Job
        Job tfJob = Job.getInstance(conf, "term frequency");
        tfJob.setJarByClass(Stats.class);
        tfJob.setMapperClass(TFMap.class);
        tfJob.setReducerClass(TFReduce.class);
        tfJob.setMapOutputKeyClass(Text.class);
        tfJob.setMapOutputValueClass(Text.class);
        tfJob.setOutputKeyClass(Text.class);
        tfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(tfJob, wordCountDir);
        FileOutputFormat.setOutputPath(tfJob, tfDir);
        tfJob.waitForCompletion(true);

        // TF-IDF Job
        Job tfidfJob = Job.getInstance(conf, "TF-IDF");
        tfidfJob.setJarByClass(Stats.class);
        tfidfJob.setMapperClass(TFIDFMap.class);
        tfidfJob.setReducerClass(TFIDFReduce.class);
        tfidfJob.setMapOutputKeyClass(Text.class);
        tfidfJob.setMapOutputValueClass(Text.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(tfidfJob, tfDir);
        FileOutputFormat.setOutputPath(tfidfJob, tfidfDir);
        tfidfJob.waitForCompletion(true);
    }
}
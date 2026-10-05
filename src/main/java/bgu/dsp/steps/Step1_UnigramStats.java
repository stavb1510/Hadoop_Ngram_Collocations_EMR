package bgu.dsp.steps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import bgu.dsp.utils.TokenFilter;

import bgu.dsp.utils.Stopwords;

import java.io.IOException;

/**
 * Step 1 (SequenceFile):
 * Reads Google 1-grams and outputs:
 *
 * 1) Unigrams:
 *    (decade, word) -> c(word, decade)
 *    Written to: <output>/unigrams/part-*
 *
 * 2) Totals:
 *    (decade, __TOTAL__) -> N(decade)
 *    Written to: <output>/totals/part-*
 *
 * Output key format (Text): "<decade>\t<token>"
 * Output value: LongWritable count
 *
 * Input (SequenceFile<LongWritable, Text>) value expected format:
 *    token \t year \t match_count \t volume_count
 */
public class Step1_UnigramStats {

    public static final String TOTAL_TOKEN = "__TOTAL__";
    public static final String CONF_USE_COMBINER = "step1.useCombiner";

    public static Job buildJob(Configuration conf, String inputPath, String outputPath, boolean useCombiner)
            throws IOException {

        Job job = Job.getInstance(conf, "Step1 - UnigramStats (SequenceFile)");
        job.setJarByClass(Step1_UnigramStats.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(Step1Mapper.class);

        if (useCombiner) {
            job.setCombinerClass(Step1Combiner.class);
        }

        job.setReducerClass(Step1SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        MultipleOutputs.addNamedOutput(job, "unigrams", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "totals", TextOutputFormat.class, Text.class, LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().setBoolean(CONF_USE_COMBINER, useCombiner);
        return job;
    }

    /**
     * Mapper (SequenceFile):
     * key: Text (ignored)
     * value: Text => "token \t year \t match_count \t volume_count"
     */
    public static class Step1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Stopwords stopwords;
        private final Text outKey = new Text();
        private final LongWritable outVal = new LongWritable();
    
        @Override
        protected void setup(Context context) {
            stopwords = new Stopwords();
        }
    
    
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null) return;
    
            String line = value.toString().trim();
            if (line.isEmpty()) return;
    
            // expected: token \t year \t match_count \t volume_count
            String[] parts = line.split("\t");
            if (parts.length < 3) return;
    
            String token = parts[0].trim();
            if (!TokenFilter.isValidWord(token)) return;
    
            token = token.toLowerCase();
            if (stopwords.isStopWord(token)) return;
    
            int year;
            long matchCount;
            try {
                year = Integer.parseInt(parts[1].trim());
                matchCount = Long.parseLong(parts[2].trim());
            } catch (NumberFormatException e) {
                return;
            }
    
            if (matchCount <= 0) return;
    
            int decade = (year / 10) * 10;
    
            // (decade, token) -> matchCount
            outKey.set(decade + "\t" + token);
            outVal.set(matchCount);
            context.write(outKey, outVal);
    
            // (decade, __TOTAL__) -> matchCount
            outKey.set(decade + "\t" + TOTAL_TOKEN);
            context.write(outKey, outVal);
        }
    }

    /**
     * Combiner: sums per key locally.
     */
    public static class Step1Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable outVal = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long sum = 0L;
            for (LongWritable v : values) sum += v.get();
            outVal.set(sum);
            context.write(key, outVal);
        }
    }

    /**
     * Reducer: sums and routes to MultipleOutputs.
     */
    public static class Step1SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable outVal = new LongWritable();
        private MultipleOutputs<Text, LongWritable> mos;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long sum = 0L;
            for (LongWritable v : values) sum += v.get();
            outVal.set(sum);

            String k = key.toString();
            String[] parts = k.split("\t", 2);
            if (parts.length < 2) return;

            String token = parts[1];

            if (TOTAL_TOKEN.equals(token)) {
                mos.write("totals", key, outVal, "totals/part");
            } else {
                mos.write("unigrams", key, outVal, "unigrams/part");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (mos != null) mos.close();
        }
    }
}
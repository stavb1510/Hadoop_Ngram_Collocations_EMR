package bgu.dsp.steps;

import bgu.dsp.comparators.JoinGroupingComparator;
import bgu.dsp.comparators.JoinSortComparator;
import bgu.dsp.partitioners.JoinPartitioner;
import bgu.dsp.utils.Stopwords;
import bgu.dsp.writables.JoinKeyWritable;
import bgu.dsp.writables.JoinValueWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import bgu.dsp.utils.TokenFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Step2_JoinC1 {


    // --- Mapper 1: Unigrams (From Step 1 Output TEXT) ---
    // Input line: "1990 \t cloud \t 50"
    public static class UnigramMapper extends Mapper<LongWritable, Text, JoinKeyWritable, JoinValueWritable> {

        private final JoinKeyWritable outKey = new JoinKeyWritable();
        private final JoinValueWritable outValue = new JoinValueWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            if (parts.length < 3) return;

            String decadeStr = parts[0].trim();
            String word = parts[1].trim();
            String countStr = parts[2].trim();

            if (word.equals(Step1_UnigramStats.TOTAL_TOKEN)) return;


            try {
                int decade = Integer.parseInt(decadeStr);
                long count = Long.parseLong(countStr);

                outKey.set(decade, word, JoinKeyWritable.TAG_UNIGRAM);
                outValue.setC1(count);

                context.write(outKey, outValue);
            } catch (NumberFormatException ignored) {
            }
        }
    }

    // --- Mapper 2: Bigrams (From SequenceFile<LongWritable, Text>) ---
    // value expected: "cloud computing \t 1990 \t 20 \t 5"  (tabs)
    public static class BigramMapper extends Mapper<LongWritable, Text, JoinKeyWritable, JoinValueWritable> {

        private final JoinKeyWritable outKey = new JoinKeyWritable();
        private final JoinValueWritable outValue = new JoinValueWritable();
        private Stopwords stopwords;
    
        @Override
        protected void setup(Context context) {
            stopwords = new Stopwords();
        }
    
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null) return;
    
            String line = value.toString().trim();
            if (line.isEmpty()) return;
    
            // expected: "w1 w2 \t year \t match_count \t volume_count"
            String[] parts = line.split("\t");
            if (parts.length < 3) return;
    
            String bigram = parts[0].trim();
            String yearStr = parts[1].trim();
            String countStr = parts[2].trim();
    
            // split "w1 w2" (allow multiple spaces)
            String[] ws = bigram.split("\\s+");
            if (ws.length != 2) return;
    
            String w1 = ws[0].toLowerCase();
            String w2 = ws[1].toLowerCase();

            if (!TokenFilter.isValidWord(w1) || !TokenFilter.isValidWord(w2)) return;
    
            // stopwords filtering
            if (stopwords.isStopWord(w1) || stopwords.isStopWord(w2)) return;
    
            int year;
            long c12;
            try {
                year = Integer.parseInt(yearStr);
                c12 = Long.parseLong(countStr);
            } catch (NumberFormatException e) {
                return;
            }
    
            if (c12 <= 0) return;
    
            int decade = (year / 10) * 10;
    
            // join on w1
            outKey.set(decade, w1, JoinKeyWritable.TAG_BIGRAM);
            outValue.setBigram(w2, c12);
    
            context.write(outKey, outValue);
        }
    }

    // --- Reducer ---
    public static class JoinReducer extends Reducer<JoinKeyWritable, JoinValueWritable, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        public void reduce(JoinKeyWritable key,
                        Iterable<JoinValueWritable> values,
                        Context context) throws IOException, InterruptedException {

            long c1 = -1;

            Map<String, Long> w2ToC12 = new HashMap<>();

            for (JoinValueWritable val : values) {

                if (val.getTag() == JoinValueWritable.TAG_C1) {
                    c1 = val.getCount();

                } else if (val.getTag() == JoinValueWritable.TAG_BIGRAM) {
                    String w2 = val.getW2().toString();
                    long c12 = val.getC12();

                    w2ToC12.merge(w2, c12, Long::sum);
                }
            }

            if (c1 < 0) return;

            String w1 = key.getJoinWord().toString();
            int decade = key.getDecade();

            for (Map.Entry<String, Long> entry : w2ToC12.entrySet()) {
                String w2 = entry.getKey();
                long summedC12 = entry.getValue();

                outKey.set(decade + "\t" + w2);
                outValue.set(w1 + "\t" + summedC12 + "\t" + c1);

                context.write(outKey, outValue);
            }
        }
    }

    public static Job buildJob(Configuration conf, String unigramInputPath, String bigramInputPath, String outputPath)
            throws IOException {

        Job job = Job.getInstance(conf, "Step 2: Join C1");
        job.setJarByClass(Step2_JoinC1.class);

        // Unigrams = TEXT output from Step1
        MultipleInputs.addInputPath(job, new Path(unigramInputPath),
                TextInputFormat.class, UnigramMapper.class);

        // Bigrams = SequenceFile<Text,Text>
        MultipleInputs.addInputPath(job, new Path(bigramInputPath),
                SequenceFileInputFormat.class, BigramMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(JoinKeyWritable.class);
        job.setMapOutputValueClass(JoinValueWritable.class);

        job.setPartitionerClass(JoinPartitioner.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);
        job.setSortComparatorClass(JoinSortComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }
}
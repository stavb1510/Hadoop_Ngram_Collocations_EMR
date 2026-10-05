package bgu.dsp.steps;

import bgu.dsp.comparators.JoinGroupingComparator;
import bgu.dsp.comparators.JoinSortComparator;
import bgu.dsp.partitioners.JoinPartitioner;
import bgu.dsp.writables.BigramStatWritable;
import bgu.dsp.writables.JoinKeyWritable;
import bgu.dsp.writables.JoinValueWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Step 3:
 * Joins Step2 partial records with C2 (unigram count of the second word),
 * loads N(decade) from a dedicated totals directory, and computes LLR.
 *
 * Inputs:
 *  - Step2 output (partials):
 *      decade \t w2 \t w1 \t c12 \t c1
 *
 *  - Step1 unigrams output (for C2):
 *      decade \t word \t c(word,decade)
 *
 *  - Step1 totals output (for N):
 *      decade \t __TOTAL__ \t N(decade)
 *    (Written by Step1 using MultipleOutputs into a "totals/" subdirectory)
 *
 * Output:
 *  Key:   decade
 *  Value: w1 \t w2 \t llrScore   (BigramStatWritable)
 */
public class Step3_JoinC2AndLLR {

    /**
     * Mapper 1:
     * Reads Step2 output (partials).
     *
     * Input format:
     *   decade \t w2 \t w1 \t c12 \t c1
     *
     * Emits:
     *   Key:   (decade, w2, tagOrder=PARTIAL)  -> tagOrder must be 1
     *   Value: PARTIAL(w1, w2, c12, c1)
     */
    public static class Step2Mapper extends Mapper<LongWritable, Text, JoinKeyWritable, JoinValueWritable> {

        private final JoinKeyWritable outKey = new JoinKeyWritable();
        private final JoinValueWritable outValue = new JoinValueWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 5) return;

            try {
                int decade = Integer.parseInt(parts[0].trim());
                String w2 = parts[1].trim();
                String w1 = parts[2].trim();
                long c12 = Long.parseLong(parts[3].trim());
                long c1 = Long.parseLong(parts[4].trim());

                outKey.set(decade, w2, JoinKeyWritable.TAG_PARTIAL);
                outValue.setPartial(w1, w2, c12, c1);

                context.write(outKey, outValue);
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Mapper 2:
     * Reads Step1 unigram output to obtain C2.
     *
     * Input format:
     *   decade \t word \t count
     *
     * Emits:
     *   Key:   (decade, word, tagOrder=UNIGRAM) -> tagOrder must be 0
     *   Value: C2(count)
     */
    public static class UnigramMapper extends Mapper<LongWritable, Text, JoinKeyWritable, JoinValueWritable> {

        private final JoinKeyWritable outKey = new JoinKeyWritable();
        private final JoinValueWritable outValue = new JoinValueWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) return;

            String token = parts[1].trim();
            if (Step1_UnigramStats.TOTAL_TOKEN.equals(token)) return; // totals are loaded separately

            try {
                int decade = Integer.parseInt(parts[0].trim());
                long c2 = Long.parseLong(parts[2].trim());

                outKey.set(decade, token, JoinKeyWritable.TAG_UNIGRAM);
                outValue.setC2(c2);

                context.write(outKey, outValue);
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Reducer:
     * For each group (decade, w2), receives first the C2 record (due to tagOrder),
     * then many PARTIAL records, and computes LLR for each (w1,w2).
     */
    public static class LLRReducer extends Reducer<JoinKeyWritable, JoinValueWritable, Text, BigramStatWritable> {

        /** N(decade) loaded once per reducer in setup from the totals directory */
        private final Map<Integer, Long> decadeTotals = new HashMap<>();

        private final Text outKey = new Text();
        private final BigramStatWritable outValue = new BigramStatWritable();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String totalsPathStr = conf.get("step1.totals.path");
            if (totalsPathStr == null) return;

            Path totalsPath = new Path(totalsPathStr);
            FileSystem fs = totalsPath.getFileSystem(conf);

            for (FileStatus status : fs.listStatus(totalsPath)) {
                Path p = status.getPath();
                if (!p.getName().startsWith("part")) continue;

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // Expected: decade \t __TOTAL__ \t N
                        String[] parts = line.split("\t");
                        if (parts.length < 3) continue;

                        int decade = Integer.parseInt(parts[0].trim());
                        long totalN = Long.parseLong(parts[2].trim());
                        decadeTotals.put(decade, totalN);
                    }
                }
            }
        }

        @Override
        public void reduce(JoinKeyWritable key, Iterable<JoinValueWritable> values, Context context)
                throws IOException, InterruptedException {

            Long N = decadeTotals.get(key.getDecade());
            if (N == null) return;

            long c2 = -1;

            for (JoinValueWritable val : values) {

                if (val.getTag() == JoinValueWritable.TAG_C2) {
                    c2 = val.getCount();
                    continue;
                }

                if (val.getTag() == JoinValueWritable.TAG_PARTIAL) {
                    if (c2 == -1) {
                        continue;
                    }

                    long c12 = val.getC12();
                    long c1 = val.getC1();

                    double llr = calcLLR(c12, c1, c2, N);

                    // Output: key=decade, value=(w1,w2,llr)
                    outKey.set(String.valueOf(key.getDecade()));
                    outValue.set(val.getW1().toString(), val.getW2().toString(), llr);

                    context.write(outKey, outValue);
                }
            }
        }

        /**
         * LLR formula (Dunning, 1993) for bigram association:
         * Uses a 2x2 contingency table:
         *
         *          w2     ~w2
         *  w1      k11    k21
         *  ~w1     k12    k22
         */
        private double calcLLR(long c12, long c1, long c2, long N) {

            double k11 = c12;
            double k12 = c2 - c12;
            double k21 = c1 - c12;
            double k22 = N - (c1 + c2 - c12);

            if (k11 <= 0 || k12 <= 0 || k21 <= 0 || k22 <= 0) return 0.0;

            return 2.0 * (
                    denormEntropy(k11, k12, k21, k22)
                  - denormEntropy(k11 + k12, k21 + k22)
                  - denormEntropy(k11 + k21, k12 + k22)
                  + denormEntropy(k11 + k12 + k21 + k22)
            );
        }

        /**
         * "Denormalized entropy" helper:
         * H(counts) = sum_i x_i log x_i - (sum_i x_i) log(sum_i x_i)
         */
        private double denormEntropy(double... xs) {
            double sum = 0.0;
            double res = 0.0;
            for (double x : xs) {
                sum += x;
                res += x * Math.log(x);
            }
            return res - sum * Math.log(sum);
        }
    }

    /**
     * Job builder.
     *
     * @param step2InputPath     Step2 output directory (partials)
     * @param step1UnigramsPath  Step1 unigrams directory (C2 source)  e.g. .../step1_v1/unigrams
     * @param step1TotalsPath    Step1 totals directory (N source)    e.g. .../step1_v1/totals
     * @param outputPath         Step3 output directory
     */
    public static Job buildJob(Configuration conf,
                               String step2InputPath,
                               String step1UnigramsPath,
                               String step1TotalsPath,
                               String outputPath) throws IOException {

        // Pass totals directory path to reducers (they will load N(decade) in setup)
        conf.set("step1.totals.path", step1TotalsPath);

        Job job = Job.getInstance(conf, "Step 3: Join C2 & Calc LLR");
        job.setJarByClass(Step3_JoinC2AndLLR.class);

        // MultipleInputs: Step2 partials + Step1 unigrams
        MultipleInputs.addInputPath(job, new Path(step2InputPath),
                TextInputFormat.class, Step2Mapper.class);

        MultipleInputs.addInputPath(job, new Path(step1UnigramsPath),
                TextInputFormat.class, UnigramMapper.class);

        job.setReducerClass(LLRReducer.class);

        // Secondary sort / reduce-side join configuration
        job.setMapOutputKeyClass(JoinKeyWritable.class);
        job.setMapOutputValueClass(JoinValueWritable.class);

        job.setPartitionerClass(JoinPartitioner.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);
        job.setSortComparatorClass(JoinSortComparator.class);

        // Final output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BigramStatWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }
}
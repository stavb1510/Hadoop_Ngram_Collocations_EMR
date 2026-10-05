package bgu.dsp.steps;

import bgu.dsp.writables.BigramStatWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Step 4:
 * Produces Top-100 collocations per decade in descending LLR order.
 *
 * Input (from Step3 text output):
 *   decade \t w1 \t w2 \t llr
 *
 * Output:
 *   decade \t rank \t w1 \t w2 \t llr
 *
 * Local aggregation option:
 * - When useCombiner=true, we enable a Combiner that keeps only Top-K per decade.
 * - Output should be identical with/without combiner (only performance differs).
 */
public class Step4_Top100PerDecade {

    public static final int K = 100;

    /**
     * Comparator for "min-heap Top-K":
     * - smallest LLR first (so pq.peek() is the current worst among the kept items)
     * - tie-break by (w1, w2) to be deterministic
     */
    private static final Comparator<BigramStatWritable> MIN_HEAP_CMP =
            (a, b) -> {
                int cmp = Double.compare(a.getLlr(), b.getLlr()); // ascending llr
                if (cmp != 0) return cmp;
                cmp = a.getW1().toString().compareTo(b.getW1().toString());
                if (cmp != 0) return cmp;
                return a.getW2().toString().compareTo(b.getW2().toString());
            };

    /**
     * Comparator for final output:
     * - highest LLR first
     * - tie-break by (w1, w2)
     */
    private static final Comparator<BigramStatWritable> FINAL_SORT_CMP =
            (a, b) -> {
                int cmp = Double.compare(b.getLlr(), a.getLlr()); // descending llr
                if (cmp != 0) return cmp;
                cmp = a.getW1().toString().compareTo(b.getW1().toString());
                if (cmp != 0) return cmp;
                return a.getW2().toString().compareTo(b.getW2().toString());
            };

    /**
     * Mapper:
     * Reads each Step3 output line and emits:
     *   key = decade
     *   value = (w1, w2, llr)
     */
    public static class Step4Mapper extends Mapper<LongWritable, Text, IntWritable, BigramStatWritable> {

        private final IntWritable outKey = new IntWritable();
        private final BigramStatWritable outVal = new BigramStatWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Expected: decade \t w1 \t w2 \t llr
            String[] parts = line.split("\t");
            if (parts.length < 4) return;

            try {
                int decade = Integer.parseInt(parts[0].trim());
                String w1 = parts[1].trim();
                String w2 = parts[2].trim();
                double llr = Double.parseDouble(parts[3].trim());

                outKey.set(decade);
                outVal.set(w1, w2, llr);
                context.write(outKey, outVal);

            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Combiner (local aggregation):
     * Keeps only Top-K per decade.
     *
     * Note:
     * Hadoop reuses value objects, so we must copy fields when we store elements.
     */
    public static class TopKCombiner extends Reducer<IntWritable, BigramStatWritable,
                                                     IntWritable, BigramStatWritable> {

        @Override
        public void reduce(IntWritable decade, Iterable<BigramStatWritable> values, Context context)
                throws IOException, InterruptedException {

            PriorityQueue<BigramStatWritable> pq = new PriorityQueue<>(K, MIN_HEAP_CMP);

            for (BigramStatWritable v : values) {
                BigramStatWritable copy =
                        new BigramStatWritable(v.getW1().toString(), v.getW2().toString(), v.getLlr());

                if (pq.size() < K) {
                    pq.add(copy);
                } else if (MIN_HEAP_CMP.compare(copy, pq.peek()) > 0) {
                    // copy is better than the current worst
                    pq.poll();
                    pq.add(copy);
                }
            }

            // IMPORTANT: PQ iteration is not sorted. We just emit the kept candidates.
            for (BigramStatWritable x : pq) {
                context.write(decade, x);
            }
        }
    }

    /**
     * Final Reducer:
     * Computes the final Top-K per decade, sorts descending, and prints rank.
     */
    public static class TopKFinalReducer extends Reducer<IntWritable, BigramStatWritable, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void reduce(IntWritable decade, Iterable<BigramStatWritable> values, Context context)
                throws IOException, InterruptedException {

            PriorityQueue<BigramStatWritable> pq = new PriorityQueue<>(K, MIN_HEAP_CMP);

            for (BigramStatWritable v : values) {
                BigramStatWritable copy =
                        new BigramStatWritable(v.getW1().toString(), v.getW2().toString(), v.getLlr());

                if (pq.size() < K) {
                    pq.add(copy);
                } else if (MIN_HEAP_CMP.compare(copy, pq.peek()) > 0) {
                    pq.poll();
                    pq.add(copy);
                }
            }

            List<BigramStatWritable> top = new ArrayList<>(pq);
            top.sort(FINAL_SORT_CMP);

            int rank = 1;
            for (BigramStatWritable x : top) {
                outKey.set(String.valueOf(decade.get()));
                outVal.set(rank + "\t" + x.getW1() + "\t" + x.getW2() + "\t" + x.getLlr());
                context.write(outKey, outVal);
                rank++;
            }
        }
    }

    /**
     * Job builder.
     *
     * @param useCombiner true => enable combiner (local aggregation), false => no combiner
     */
    public static Job buildJob(Configuration conf, String inputPath, String outputPath, boolean useCombiner)
            throws IOException {

        Job job = Job.getInstance(conf, "Step4 - Top100 per decade");
        job.setJarByClass(Step4_Top100PerDecade.class);

        job.setMapperClass(Step4Mapper.class);

        // Local aggregation (toggle for measurements)
        if (useCombiner) {
            job.setCombinerClass(TopKCombiner.class);
        }

        job.setReducerClass(TopKFinalReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BigramStatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}
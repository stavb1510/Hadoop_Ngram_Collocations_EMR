package bgu.dsp.jobs;

import bgu.dsp.steps.Step1_UnigramStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Job1 wrapper: runs Step1_UnigramStats.
 *
 * Usage:
 *   Job1 <inputPath> <outputPath> <useCombiner>
 *
 * Example:
 *   Job1 input/onegrams output/step1 true
 */
public class Job1 {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: Job1 <inputPath> <outputPath> <useCombiner>");
            System.exit(2);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        boolean useCombiner = Boolean.parseBoolean(args[2]);

        Configuration conf = new Configuration();

        Job job = Step1_UnigramStats.buildJob(conf, inputPath, outputPath, useCombiner);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
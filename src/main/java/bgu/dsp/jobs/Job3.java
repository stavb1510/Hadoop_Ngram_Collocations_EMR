package bgu.dsp.jobs;

import bgu.dsp.steps.Step3_JoinC2AndLLR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Job3 wrapper: runs Step3_JoinC2AndLLR.
 *
 * Usage:
 *   Job3 <step2Output> <step1UnigramsPath> <step1TotalsPath> <step3Output>
 */
public class Job3 {
    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: Job3 <step2Output> <step1UnigramsPath> <step1TotalsPath> <step3Output>");
            System.exit(1);
        }

        String step2Output = args[0];
        String step1Unigrams = args[1];
        String step1Totals = args[2];
        String step3Output = args[3];

        Configuration conf = new Configuration();

        Job job = Step3_JoinC2AndLLR.buildJob(conf, step2Output, step1Unigrams, step1Totals, step3Output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
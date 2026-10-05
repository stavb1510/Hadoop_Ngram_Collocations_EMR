package bgu.dsp.jobs;

import bgu.dsp.steps.Step4_Top100PerDecade;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Job4 wrapper: runs Step4_Top100PerDecade.
 *
 * Usage:
 *   hadoop jar yourJar.jar bgu.dsp.jobs.Job4 <step3Output> <step4Output> <useCombiner:true|false>
 *
 * Examples:
 *   ... Job4 s3://.../step3_out s3://.../step4_out_with true
 *   ... Job4 s3://.../step3_out s3://.../step4_out_without false
 */
public class Job4 {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Job4 <step3Output> <step4Output> <useCombiner:true|false>");
            System.exit(1);
        }

        String step3Output = args[0];
        String step4Output = args[1];

        String flag = args[2].trim().toLowerCase();
        boolean useCombiner;
        if ("true".equals(flag)) useCombiner = true;
        else if ("false".equals(flag)) useCombiner = false;
        else {
            System.err.println("Invalid useCombiner value: " + args[2] + " (expected true/false)");
            System.exit(1);
            return;
        }

        System.out.println("Job4 starting. useCombiner=" + useCombiner);

        Configuration conf = new Configuration();
        Job job = Step4_Top100PerDecade.buildJob(conf, step3Output, step4Output, useCombiner);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
package bgu.dsp.jobs;

import bgu.dsp.steps.Step2_JoinC1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class Job2 {
    public static void main(String[] args) throws Exception {
        // args[0] = Step 1 Output Path (Unigrams)
        // args[1] = Bigrams Input Path
        // args[2] = Step 2 Output Path
        
        if (args.length < 3) {
            System.err.println("Usage: Job2 <unigramInput> <bigramInput> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        
        Job job = Step2_JoinC1.buildJob(conf, args[0], args[1], args[2]);
        
        if (job.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
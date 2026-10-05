package bgu.dsp.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class EmrRunner {

    public static void main(String[] args) {

        if (args.length != 1) {
            System.err.println("Usage: EmrRunner <useCombiner:true|false>");
            System.exit(1);
        }

        boolean useCombiner = Boolean.parseBoolean(args[0]);

        // --- 1. CONFIGURATION ---
        String bucketName = "kfir-stav-bucket";
        String keyPairName = "vnode";

        String jarUrl = "s3://" + bucketName + "/jars/assignment2.jar";
        String logUri = "s3://" + bucketName + "/logs/";

        // --- 2. INPUT PATHS (uploaded files) ---
        //String input_Onegrams ="s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data";

        //String input_Bigrams ="s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data";

        String input_Onegrams ="s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";

        String input_Bigrams ="s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";

        // --- 3. OUTPUT PATHS ---
        // We create a unique "run" prefix based on the combiner flag
        String runTag = useCombiner ? "withCombiner" : "noCombiner";
        String baseOut = "s3://" + bucketName + "/output/run_" + runTag + "_v1";

        // Step1 base output directory:
        String output_Step1_Base = baseOut + "/step1";

        // Step1 subdirectories created by MultipleOutputs:
        String output_Step1_Unigrams = output_Step1_Base + "/unigrams";
        String output_Step1_Totals   = output_Step1_Base + "/totals";

        // Step2 output:
        String output_Step2 = baseOut + "/step2";

        // Step3 output:
        String output_Step3 = baseOut + "/step3";

        // Step4 output:
        String output_Step4 = baseOut + "/step4";

        System.out.println("Connecting to AWS EMR...");
        System.out.println("Run mode: " + runTag);
        System.out.println("Base output: " + baseOut);

        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_EAST_1)
                .build();

        // --- Step 1: Unigram Stats ---
        // Job1 args (UPDATED): <inputPath> <outputPath> <useCombiner:true|false>
        HadoopJarStepConfig step1Jar = new HadoopJarStepConfig()
                .withJar(jarUrl)
                .withMainClass("bgu.dsp.jobs.Job1")
                .withArgs(input_Onegrams, output_Step1_Base, String.valueOf(useCombiner));

        StepConfig step1Config = new StepConfig()
                .withName("Step 1: Unigram Stats (" + runTag + ")")
                .withHadoopJarStep(step1Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // --- Step 2: Join C1 ---
        HadoopJarStepConfig step2Jar = new HadoopJarStepConfig()
                .withJar(jarUrl)
                .withMainClass("bgu.dsp.jobs.Job2")
                .withArgs(output_Step1_Unigrams, input_Bigrams, output_Step2);

        StepConfig step2Config = new StepConfig()
                .withName("Step 2: Join C1 (" + runTag + ")")
                .withHadoopJarStep(step2Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // --- Step 3: Join C2 + Totals and compute LLR ---
        HadoopJarStepConfig step3Jar = new HadoopJarStepConfig()
                .withJar(jarUrl)
                .withMainClass("bgu.dsp.jobs.Job3")
                .withArgs(output_Step2, output_Step1_Unigrams, output_Step1_Totals, output_Step3);

        StepConfig step3Config = new StepConfig()
                .withName("Step 3: Calc LLR (" + runTag + ")")
                .withHadoopJarStep(step3Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // --- Step 4: Top-100 per decade ---
        // Job4 args: <step3Output> <step4Output> <useCombiner:true|false>
        HadoopJarStepConfig step4Jar = new HadoopJarStepConfig()
                .withJar(jarUrl)
                .withMainClass("bgu.dsp.jobs.Job4")
                .withArgs(output_Step3, output_Step4, String.valueOf(useCombiner));

        StepConfig step4Config = new StepConfig()
                .withName("Step 4: Top-100 (" + runTag + ")")
                .withHadoopJarStep(step4Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // --- 4. HARDWARE CONFIGURATION ---
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(6)
                .withMasterInstanceType("m5.xlarge")
                .withSlaveInstanceType("m5.xlarge")
                .withEc2KeyName(keyPairName)
                .withKeepJobFlowAliveWhenNoSteps(false);

        // --- 5. SUBMIT JOB FLOW ---
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DSP_Ass2_Run_" + runTag)
                .withReleaseLabel("emr-6.10.0")
                .withInstances(instances)
                .withSteps(step1Config, step2Config, step3Config, step4Config)
                .withLogUri(logUri)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        System.out.println("Submitting Job Flow (4 steps, single run)...");
        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);

        System.out.println("Job Submitted! ID: " + runJobFlowResult.getJobFlowId());
        System.out.println("Check status in the EMR console.");
    }
}
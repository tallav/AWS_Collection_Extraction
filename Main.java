package aws_ass2;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class Main {

	public static String LANGUAGE;
	public static final String EMR_RELEASE_LABEL = "emr-5.14.0"; 
	public static final String HADOOP_VERSION = "2.8.3";
	public static final String EC2_KEY_NAME = "keypair2";
	
	public static final String REGION = "us-east-1d";
	public static final String SUBNET_ID = "subnet-eab969a0";
	public static final int NUM_OF_INSTANCES = 10;
	public static final String INSTANCE_TYPE = InstanceType.M3Xlarge.toString();
	
	public static final String BUCKET_NAME = "awsass2";
	public static final String BUCKET_URL = "s3n://" + BUCKET_NAME + "/";
	public static final String LOG_DIR = BUCKET_URL + "Logs/";

	public static String ONE_GRAM_BUCKET_URL;
	public static String TWO_GRAM_BUCKET_URL;
	public static String STOP_WORDS;
	public static final String HEB_STOP_WORDS = BUCKET_URL + "HebrewStopWords.txt";
	public static final String ENG_STOP_WORDS = BUCKET_URL + "EnglishStopWords.txt";
	
	public static final String STEP0_JAR = BUCKET_URL + "JARs/Step0Tal.jar";
	public static final String STEP1_JAR = BUCKET_URL + "JARs/Step1Tal.jar";
	public static final String STEP2_JAR = BUCKET_URL + "JARs/Step2Tal.jar";
	public static final String STEP3_JAR = BUCKET_URL + "JARs/Step3Tal.jar";
	public static final String STEP4_JAR = BUCKET_URL + "JARs/Step4Tal.jar";
	public static final String STEP5_JAR = BUCKET_URL + "JARs/Step5_withNaN.jar";
	public static final String STEP6_JAR = BUCKET_URL + "JARs/Step6_31.7.jar";
	
	public static final String STEP0_OUTPUT = BUCKET_URL + "Outputs/out0";
	public static final String STEP1_OUTPUT = BUCKET_URL + "Outputs/out1";
	public static final String STEP2_OUTPUT = BUCKET_URL + "Outputs/out2";
	public static final String STEP3_OUTPUT = BUCKET_URL + "Outputs/out3";
	public static final String STEP4_OUTPUT = BUCKET_URL + "Outputs/out4";
	public static final String STEP5_OUTPUT = BUCKET_URL + "Outputs/out5";
	public static final String STEP6_OUTPUT = BUCKET_URL + "Outputs/out6";
	
	

	public static void main(String[] args) {
		LANGUAGE = args[0];
		if(LANGUAGE.equals("eng")){
			ONE_GRAM_BUCKET_URL = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data";
			TWO_GRAM_BUCKET_URL = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
			STOP_WORDS = ENG_STOP_WORDS;
		} else if(LANGUAGE.equals("heb")){
			ONE_GRAM_BUCKET_URL = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
			TWO_GRAM_BUCKET_URL = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
			STOP_WORDS = HEB_STOP_WORDS;
		} else{
			System.out.println("Ilegal Language");
			return;
		}
	    AWSCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
	    AWSCredentials credentials = credentialsProvider.getCredentials();
	    System.out.println("Retrieved credentials");
	    AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
	    
	    StepConfig stepConfigDebug = new StepConfig()
	    	    .withName("Enable Debugging")
	    	    .withActionOnFailure("TERMINATE_JOB_FLOW")
	    	    .withHadoopJarStep(new HadoopJarStepConfig()
	    	    .withJar("command-runner.jar")
	    	    .withArgs("state-pusher-script"));
	    
	    // Configure Step0
	    HadoopJarStepConfig hadoopJarStep0 = new HadoopJarStepConfig()
	        .withJar(STEP0_JAR) 
	        .withArgs("step0", "step0", ONE_GRAM_BUCKET_URL, STOP_WORDS, STEP0_OUTPUT);
	     
	    StepConfig stepConfig0 = new StepConfig()
	        .withName("step0")
	        .withHadoopJarStep(hadoopJarStep0);
	    
	    // Configure Step1
	    HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
	        .withJar(STEP1_JAR) 
	        .withArgs("step1", "step1", STEP0_OUTPUT, STEP1_OUTPUT);
	     
	    StepConfig stepConfig1 = new StepConfig()
	        .withName("step1")
	        .withHadoopJarStep(hadoopJarStep1);
	    
	    // Configure Step2
	    HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
	        .withJar(STEP2_JAR) 
	        .withArgs("step2", "step2", TWO_GRAM_BUCKET_URL, STEP2_OUTPUT);
	     
	    StepConfig stepConfig2 = new StepConfig()
	        .withName("step2")
	        .withHadoopJarStep(hadoopJarStep2);
	    
	    // Configure Step3
	    HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
	        .withJar(STEP3_JAR) 
	        .withArgs("step3", "step3", STEP0_OUTPUT, STEP2_OUTPUT,STEP3_OUTPUT);
	     
	    StepConfig stepConfig3 = new StepConfig()
	        .withName("step3")
	        .withHadoopJarStep(hadoopJarStep3);
	    
	    // Configure Step4
	    HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
	        .withJar(STEP4_JAR) 
	        .withArgs("step4", "step4", STEP3_OUTPUT, STEP4_OUTPUT);
	     
	    StepConfig stepConfig4 = new StepConfig()
	        .withName("step4")
	        .withHadoopJarStep(hadoopJarStep4);
	    
	    // Configure Step5
	    HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
	        .withJar(STEP5_JAR) 
	        .withArgs("step5", "step5", STEP4_OUTPUT, STEP5_OUTPUT, STEP1_OUTPUT);
	     
	    StepConfig stepConfig5 = new StepConfig()
	        .withName("step5")
	        .withHadoopJarStep(hadoopJarStep5);
	    
	    // Configure Step6
	    HadoopJarStepConfig hadoopJarStep6 = new HadoopJarStepConfig()
	        .withJar(STEP6_JAR) 
	        .withArgs("step6", "step6", STEP5_OUTPUT, STEP6_OUTPUT);
	     
	    StepConfig stepConfig6 = new StepConfig()
	        .withName("step6")
	        .withHadoopJarStep(hadoopJarStep6);
	    
	    System.out.println("Configured all steps");
	    
	    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
	        .withInstanceCount(NUM_OF_INSTANCES)
	        .withMasterInstanceType(INSTANCE_TYPE)
	        .withSlaveInstanceType(INSTANCE_TYPE)
	        .withHadoopVersion(HADOOP_VERSION)
	        .withEc2KeyName(EC2_KEY_NAME)
	        .withKeepJobFlowAliveWhenNoSteps(false)
	        .withEc2SubnetId(SUBNET_ID);
	    
	    // Create a flow request including all the steps
	    RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
	        .withName("CollocationExtraction")
	        .withInstances(instances)
	        .withSteps(stepConfigDebug, stepConfig0, stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5, stepConfig6)
	        .withLogUri(LOG_DIR)
	        .withServiceRole("EMR_DefaultRole")
	        .withJobFlowRole("EMR_EC2_DefaultRole")
	        .withReleaseLabel(EMR_RELEASE_LABEL);
	    
	    System.out.println("Created job flow request");
	     
	    // Run the flow
	    RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
	    String jobFlowId = runJobFlowResult.getJobFlowId();
	    System.out.println("Ran job flow with id: " + jobFlowId);
	}

}

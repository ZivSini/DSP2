import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import static java.lang.System.exit;


public class Main {
    public static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
    public static AmazonElasticMapReduce emr;

    public static void main(String[] args) {

        if(args.length < 2){
            System.out.println("Missing parameters!");
            System.out.println("Expected parameters: minPmi, relMinPmi");
            exit(1);
        }

        System.out.println("Creating EMR instance!");
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();


        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(stepsRunnerJarPath)
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data", output_path);
        StepConfig onlyStep = new StepConfig()
                .withName("all_steps")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


		/*
        Set instances
		 */

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName(key)
                .withPlacement(new PlacementType("us-east-1a"))
                .withKeepJobFlowAliveWhenNoSteps(false);

		/*
        Run all jobs
		 */
        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName(key)
                .withInstances(instances)
                .withSteps(onlyStep)
                .withLogUri(log_path)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");
        RunJobFlowResult result = emr.runJobFlow(request);
        String id = result.getJobFlowId();
        System.out.println("Id: " + id);






    }
}

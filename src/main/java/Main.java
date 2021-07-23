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

        double minPmi = Double.parseDouble(args[0]);
        double relMinPmi = Double.parseDouble(args[1]);


    }
}

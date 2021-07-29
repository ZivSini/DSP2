import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepsManager {

    private static String outputPath;
    private static String google2GramPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    private static double minNPMI;
    private static double relMinNPMI;
    private static String s3BucketPath = "https://2gram-bucket.s3.amazonaws.com/";

    private static String runStep1 (Job job) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1.Step1Mapper.class);
        job.setReducerClass(Step1.Step1Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(google2GramPath));
        String stepOutputPath = s3BucketPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (stepOutputPath));

        if(!job.waitForCompletion(true)){
            System.out.println("Job 1 failed");
            System.exit(1);
        }

        return stepOutputPath;
    }
    private static String runStep2 (Job job, String inputPath) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Step2.class);
        job.setMapperClass(Step2.Step2Mapper.class);
        job.setReducerClass(Step2.Step2Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        String stepOutputPath = s3BucketPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (stepOutputPath));

        if(!job.waitForCompletion(true)){
            System.out.println("Job 2 failed");
            System.exit(1);
        }

        return stepOutputPath;
    }
    private static String runStep3 (Job job, String inputPath) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Step3.class);
        job.setMapperClass(Step3.Step3Mapper.class);
        job.setReducerClass(Step3.Step3Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path (outputPath));

        if(!job.waitForCompletion(true)){
            System.out.println("Job 3 failed");
            System.exit(1);
        }

        return outputPath;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // First stage - split the corpus into 2 parts
        if (args.length<2){
            System.out.println("please provide input path and output path");
            System.exit(1);
        }

        outputPath = args[0];
        minNPMI = Double.parseDouble(args[1]);
        relMinNPMI = Double.parseDouble(args[2]);

//      Step 1
        Configuration job1conf = new Configuration();
        final Job job1 = Job.getInstance(job1conf, "Step1");
        String step1output = runStep1(job1);

        System.out.println("Step 1 completed successfully");


        Configuration job2conf = new Configuration();
        for (int i = minDecade; i < maxDecade; i+=10) {
            String decade = job1conf.get("N " + i);
            if(decade != null) job2conf.set("N " + i, decade);
        }
        final Job job2 = Job.getInstance(job2conf, "step2");
        String step2output = runStep2(job2, step1output);

        System.out.println("Step 2 completed successfully ");

//      Step 3
        Configuration job3conf = new Configuration();
        job3conf.set("minNPMI", String.valueOf(minNPMI));
        job3conf.set("relMinNPMI", String.valueOf(relMinNPMI));
        final Job job3 = Job.getInstance(job3conf, "step3");
        String step3output = runStep2(job3, step2output);

        System.out.println("Step 3 completed successfully ");


        Configuration job4conf = new Configuration();
        final Job job4 = Job.getInstance(job4conf, "step4");
        String job4Path = step4(job4, job3Path);
        if (job4.waitForCompletion(true)){
            System.out.println("Job 4 Completed");
        }
        else{
            System.out.println("Job 4 Failed");
            System.exit(1);
        }
    }
}

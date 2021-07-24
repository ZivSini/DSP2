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

    private static String bucketOutputPath;

    private static String setPaths (Job job, String inputPath) throws  IOException {
        FileInputFormat.addInputPath(job, new Path(inputPath));
        String path = bucketOutputPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    private static String step1 (Job job, String filePath,boolean use_combiner_or_not) throws IOException {

        job.setJarByClass(step1_count_N_and_split_corpus.class);
        job.setMapperClass(step1_count_N_and_split_corpus.MapperClass.class);
        job.setReducerClass(step1_count_N_and_split_corpus.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        if(use_combiner_or_not){
            job.setCombinerClass(step1_count_N_and_split_corpus.CombinerClass.class);
        }
        return setPaths(job, filePath);
    }
    private static String step2 (Job job, String filePath) throws IOException {
        job.setJarByClass(step2_map_calc_nr_tr_reduce_name_with_corpus_val_nr_tr.class);
        job.setMapperClass(step2_map_calc_nr_tr_reduce_name_with_corpus_val_nr_tr.Mapper_Nr_Tr.class);
        job.setReducerClass(step2_map_calc_nr_tr_reduce_name_with_corpus_val_nr_tr.ReducerClass.class);
        job.setPartitionerClass(step2_map_calc_nr_tr_reduce_name_with_corpus_val_nr_tr.PartitionerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return setPaths(job, filePath);
    }
    private static String step3 (Job job, String filePath) throws IOException {
        job.setJarByClass(step3_calculate_probability.class);
        job.setMapperClass(step3_calculate_probability.Mapper_pass_to_reducer.class);
        job.setReducerClass(step3_calculate_probability.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return setPaths(job, filePath);
    }
    private static String step4 (Job job, String filePath) throws IOException {
        job.setJarByClass(step4_sort.class);
        job.setMapperClass(step4_sort.Mapper_pass_to_reducer.class);
        job.setReducerClass(step4_sort.ReducerClass.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return setPaths(job, filePath);
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // First stage - split the corpus into 2 parts
        if (args.length<2){
            System.out.println("please provide input path and output path");
            System.exit(1);
        }

        String input = args[0];
        bucketOutputPath = args[1];
        boolean combiner_or_not = args.length > 2 && args[2].equals("yes");

        Configuration job1conf = new Configuration();
        final Job job1 = Job.getInstance(job1conf, "step1");
        String step1Path = step1(job1, input, combiner_or_not);
        if (job1.waitForCompletion(true)){
            System.out.println("Job 1 Completed");
        }
        else{
            System.out.println("Job 1 Failed");
            System.exit(1);
        }
        Counters counters = job1.getCounters();
        Counter counter = counters.findCounter(NCounter.N_COUNTER);
        long N = counter.getValue();
        job1conf.setLong("counter", N);


        Configuration job2conf = new Configuration();
        job2conf.setLong("counter", N);
        final Job job2 = Job.getInstance(job2conf, "step2");
        String job2Path = step2(job2, step1Path);

        if (job2.waitForCompletion(true)){
            System.out.println("Job 2 Completed");
            System.out.println("Counter: " + N);

        }
        else{
            System.out.println("Job 2 Failed");
            System.exit(1);
        }

        Configuration job3conf = new Configuration();
        job3conf.setLong("counter", N);
        final Job job3 = Job.getInstance(job3conf, "step3");
        String job3Path = step3(job3, job2Path);

        if (job3.waitForCompletion(true)){
            System.out.println("Job 3 Completed");
        }
        else{
            System.out.println("Job 3 Failed");
            System.exit(1);
        }


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

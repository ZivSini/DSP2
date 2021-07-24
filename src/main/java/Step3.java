import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Pattern;

public class Step3 {

    public static class Step3Mapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text val, Context context) throws IOException, InterruptedException {
            String[] valueAsArray = val.toString().split("\\s+");
            double nPMI = Double.parseDouble(valueAsArray[0]);
            String word1 = valueAsArray[1];
            String word2 = valueAsArray[2];

            double invertedNPMI = 1 - nPMI;

            Text newValue = new Text(invertedNPMI +" " +word1 +" " +word2);
            context.write(key,newValue);
        }
    }

    public static class Step3Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            String decade = key.toString();
            Configuration configuration = context.getConfiguration();
            double minNPMI = Double.parseDouble(configuration.get("minNPMI"));
            double relMinNPMI = Double.parseDouble(configuration.get("relMinNPMI"));
            int count = 0;
            double nPMISum = 0;

//            count the total sum of nPMIs for the relevant decade
            for(Text value : values){
                String[] valueAsArray = value.toString().split("\\s+");
                double invertedNPMI = Double.parseDouble(valueAsArray[0]);
                double nPMI = 1 - invertedNPMI;

                nPMISum += nPMI;
            }

//            Find the top 10 collocations for the decade () and emit them
            for(Text value : values){
                String[] valueAsArray = value.toString().split("\\s+");
                double invertedNPMI = Double.parseDouble(valueAsArray[0]);
                double nPMI = 1 - invertedNPMI;
                String word1 = valueAsArray[1];
                String word2 = valueAsArray[2];

//                Check if 10 pairs were found
                if(count >= 10) return;

//                Check absolute nPMI criteria
                if(nPMI >= minNPMI){
                    count++;

                    Text newKey = new Text(decade);
                    Text newValue = new Text(nPMI +" " +word1 +" " +word2);
                    context.write(newKey, newValue);
                }

                else if(nPMI / nPMISum >= relMinNPMI){
                    count ++;

                    Text newKey = new Text(decade);
                    Text newValue = new Text(nPMI +" " +word1 +" " +word2);
                    context.write(newKey, newValue);
                }
            }

        }
    }
}


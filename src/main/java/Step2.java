import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Pattern;

public class Step2 {

    public static class Step2Mapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text line, Text occ, Context context) throws IOException, InterruptedException {
            context.write(line,occ);
        }
    }

    public static class Step2Reducer extends Reducer<Text, Text, Text, Text> {

        public static double calculateNPMI(String decade, long c, long c1, long c2, Reducer<Text, Text, Text, Text>.Context context){
            long n = Long.parseLong(context.getConfiguration().get("N " +decade));
            double pmi = (Math.log(c) + Math.log(n) - Math.log(c1) - Math.log(c2));
            double p = (double) c / n;
            return pmi / (-Math.log(p));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            boolean isFirst = true;
            String[] keyAsArray = key.toString().split("\\s+");
            String word1 = keyAsArray[0];
            String decade = keyAsArray[1];
            String word2 = null;
            long occC = 0;
            long occC1 = 0;
            long occC2 = 0;


            for (Text val: values) {
                String[] valueAsArray = val.toString().split("\\s+");
                String currWord2 = valueAsArray[0];
                String type = valueAsArray[1];
                long currOcc = Long.parseLong(valueAsArray[2]);

                if(isFirst){
                    word2 = currWord2;
                    isFirst = false;
                    continue;
                }

                else if(!word2.equals(currWord2)){
                    double nPMI = calculateNPMI(decade, occC, occC1, occC2, context);

                    // Emit the nPMI of <w1, w2, decade>
                    Text newKey = new Text(decade);
                    Text newValue = new Text(String.valueOf(nPMI) +" " +word1 +" " +word2);

                    context.write(newKey, newValue);

                    word2 = currWord2;
                    occC = 0;
                    occC1= 0;
                    occC2 = 0;
                }

                switch (type){
                    case "C":{
                        occC += currOcc;
                    }
                    case "C1": {
                        occC1 += currOcc;
                    }
                    case "C2": {
                        occC2 += currOcc;
                    }
                    default: {
                        throw new IOException("Invalid type in step 2: " +type);
                    }
                }
            }

//          calculate the nPMI of the last 2gram
            double nPMI = calculateNPMI(decade, occC, occC1, occC2, context);

            // Emit the nPMI of <w1, w2, decade>
            Text newKey = new Text(decade);
            Text newValue = new Text(String.valueOf(nPMI) +" " +word1 +" " +word2);

            context.write(newKey, newValue);
        }
    }
}


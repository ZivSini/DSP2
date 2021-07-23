import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Pattern;

public class Step1 {

    public static class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        Pattern pattern;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) {
            pattern = Pattern.compile("[א-ת]+[א-ת]");
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] lineAsArray = line.toString().split("\\s+");

            String word1 = lineAsArray[0];
            String word2 = lineAsArray[1];
            int year =  Integer.parseInt(lineAsArray[2]);
            int decade = (year /10) *10;
            String occ =  lineAsArray[3];

            if(!pattern.matcher(word1).matches() || !pattern.matcher(word2).matches()) return;
            if(Utils.stopWords.contains(word1) || Utils.stopWords.contains(word2)) return;

            // Emit N: counter of all 2-grams
            Text nKey = new Text("N " +decade);
            Text nValue = new Text(occ);
            context.write(nKey, nValue);

            // Emit c(w1, w2)
            Text cKey = new Text("C " +decade +" " +word1 +" " +word2);
            Text cValue = new Text(occ);
            context.write(cKey, cValue);

            // Emit c(w1)
            Text c1Key = new Text("C1 " +decade +" " +word1);
            Text c1Value = new Text(occ);
            context.write(c1Key, c1Value);

            // Emit c(w2)
            Text c2Key = new Text("C2 " +decade +" " +word2);
            Text c2Value = new Text(occ);
            context.write(c2Key, c2Value);
        }
    }

    public static class Step1Reducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            long totalOcc = 0;

            for(Text val : values){
                totalOcc += Long.parseLong(val.toString());
            }

            context.write(key, new Text(String.valueOf(totalOcc)));
        }
    }
}
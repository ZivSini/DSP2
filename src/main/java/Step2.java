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
//    final static int tags = 100;

    public static class Step2Mapper extends Mapper<Text, Text, Text, Text> {


        @Override
        public void map(Text line, Text occ, Context context) throws IOException,  InterruptedException {
            String[] lineAsArray = line.toString().split("\\s+");

            String type = lineAsArray[0];
            String decade = lineAsArray[1];
            String word1 = lineAsArray.length > 2 ? lineAsArray[2] : null;
            String word2 = lineAsArray.length > 3 ? lineAsArray[3] : null;

            if(type.equals("C")){
                String key = word1 +" " +decade;
                String value = type +" " +occ +" " +word2;

                context.write(new Text(key), new Text(value));
            }

            else if(type.equals("C1")){
                String key = word1 +" " +decade;
                String value = type +" " +occ;

                context.write(new Text(key), new Text(value));
            }

//            cases of type N or C2
            else{
                context.write(line, occ);
            }

//
////           In case of N counter, send number of occurrences to all
//            if(type.equals("N")) {
//                String value = "N " + decade + " " + occ;
//
//                for (int i = 0; i < tags; i++) {
//                    context.write(new Text(String.valueOf(i)),
//                                  new Text(value));
//                }
//            }
//
////           In case of C1 counter, send number of occurrences to all
//            else if(type.equals("C1")) {
//                String value = "C1 " +decade +" " + occ +" " +word1;
//
//                for (int i = 0; i < tags; i++) {
//                    context.write(new Text(String.valueOf(i)),
//                            new Text(value));
//                }
//            }
//
////           In case of C2 counter, send number of occurrences to all
//            else if(type.equals("C2")) {
//                String value = "C2 " +decade +" " + occ +" " +word1;
//
//                for (int i = 0; i < tags; i++) {
//                    context.write(new Text(String.valueOf(i)),
//                            new Text(value));
//                }
//            }
//
////            In this case, the type must be C
//            else {
//                String bothWords = word1 +" " +word2;
//                int bothWordsHash = bothWords.hashCode() % tags;
//
//                Text key = new Text(String.valueOf(bothWordsHash));
//                Text value = new Text(type +" " +decade +" "+ occ + " " +word1 +" " +word2 );
//
//                context.write(key, value);
//            }
//        }
//    }
//
        }
    public static class Step2Reducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            String[] lineAsArray = key.toString().split("\\s+");

            String word = lineAsArray[0];

//            In this case, we behechrach get a single element in values
            if(word.equals("N") || word.equals("C2")){
                context.write(key, values.iterator().next());
            }
        }
    }
}

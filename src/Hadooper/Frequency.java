package Hadooper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import static Hadooper.Main.SCleaner;

public class Frequency {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text twoWords = new Text();
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            //ArrayList<String> TwoWordsTokens = Main.TwoWords(line);

            StringTokenizer tokenizer = new StringTokenizer(line);
            //los tw_tokenizers tienen un offset de 1 palabra entre si
            StringTokenizer tw_tokenizer1 = new StringTokenizer(line,"(?<!\\G\\S+)\\s");//regex para cada otro espacio
            StringTokenizer tw_tokenizer2 = new StringTokenizer(line.split(" ",2)[2],"(?<!\\G\\S+)\\s");

            //for (int i = 0; i < TwoWordsTokens.size(); i++) {
            //    TwoWords.set(TwoWordsTokens.get(i).toString());
            //    output.collect(TwoWords, one);
            //}
            
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
            while (tw_tokenizer1.hasMoreTokens()) {
                String count_test = tw_tokenizer1.nextToken();
                if(count_test.indexOf(' ')!=-1){
                    twoWords.set(count_test);
                    output.collect(twoWords, one);
                }
            }
            while (tw_tokenizer2.hasMoreTokens()) {
                String count_test = tw_tokenizer2.nextToken();
                if(count_test.indexOf(' ')!=-1){
                    twoWords.set(count_test);
                    output.collect(twoWords, one);
                }
            }
        }
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> { 
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

}

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
            
            //1 word
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
            
            //2 words
            StringTokenizer tw_tokenizer = new StringTokenizer(line);
            String word1 ="", word2="";
            if(tw_tokenizer.hasMoreTokens()) {
            	word1 = tw_tokenizer.nextToken();
            }
            while (tw_tokenizer.hasMoreTokens()) {
                word2 = tw_tokenizer.nextToken();
                twoWords.set(word1 + " " + word2);
                output.collect(twoWords, one);
                word1=word2;
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

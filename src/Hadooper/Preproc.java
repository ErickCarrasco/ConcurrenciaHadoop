package Hadooper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import static Hadooper.Main.SCleaner;

public class Preproc {
	public static class Preprocessor extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {
		private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
            String line = SCleaner(value.toString());    
            word.set(line);
            output.collect(word, null);
        }
	}

}

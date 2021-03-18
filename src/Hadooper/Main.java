package Hadooper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Main {
	
	static ArrayList<String> EnglishDictionary = LoadDictionary();
	public static void main(String[] args) {
		
		runPreproces(args);
		runFrequency(args);
		
	}
	public static ArrayList<String> LoadDictionary() {
        ArrayList<String> Dictionary = new ArrayList<>();
        File file = null;
        FileReader fr = null;
        BufferedReader br = null;
        try {
            
            file = new File("/home/erick/Documents/Tests/ConcurrenciaHadoop/src/words.txt");

            fr = new FileReader(file);   
            br = new BufferedReader(fr);      
            String line;

            while ((line = br.readLine()) != null) {    
                String[] word = line.split(" "); 
                Dictionary.add(word[0].toLowerCase());
            }
            fr.close();    
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        try{
            br.close();
            fr.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("Dictionary Successfully Loaded! Words Loaded: "+Dictionary.size());
        return Dictionary;
        
    }
	
	public static String WordString(String cleaner) {
		ArrayList<String> symbols = new ArrayList<String>(Arrays.asList(new String[] {"a","about", "above", "after", "again", 
				"against", "aint", "all", "am", "an", "and", "any", "are", "aren't", "arent", "as", "at", "be", 
				"because", "been", "before", "being", "below", "between", "both", "but", "by", "can", "could", 
				"couldn't", "d", "did", "didn't", "didnt", "do", "does", "doesn't", "doesnt", "doing", "don't", 
				"dont", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "hadnt", "has", 
				"hasn't", "hasnt", "have", "haven't", "havent", "having", "he", "her", "here", "hers", "herself", "him", ""
						+ "himself", "his", "how", "i", "if", "in", "into", "is", "isn", "isn't", "it", "it's", "its", 
						"itself", "just", "ll", "m", "ma", "me", "mightn't", "mightnt", "more", "most", "mustn", 
						"mustn't", "my", "myself", "needn't", "neednt", "no", "nor", "not", "now", "o", "of", "off", 
						"on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", "re", "s", 
						"same", "shan", "shan't", "she", "she's", "should", "should've", "shouldn't", "shouldnt", "so", 
						"some", "such", "t", "than", "that", "that'll", "the", "their", "theirs", "them", "themselves", 
						"then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", 
						"ve", "very", "was", "wasn't", "wasnt", "we", "were", "weren't", "werent", "what", "when", "where", 
						"which", "while", "who", "whom", "why", "will", "with", "won't", "wont", "wouldn't", "wouldnt", "y", 
				"you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"}));
		
		String[] tokens = cleaner.split(" ");
		ArrayList<String> res = new ArrayList();
		String retVal="";
		for(int i = 0; i< tokens.length; i++) {
			if(symbols.contains(tokens[i])) {
				
			}else {
				Boolean DictionaryContains = EnglishDictionary.contains(tokens[i]);
                if(tokens[i].length() > 2){
                    Boolean isHashorAt;
                    if(Character.toString(tokens[i].charAt(0)).equals("@") || Character.toString(tokens[i].charAt(0)).equals("#")){
                        isHashorAt = true;
                    } else {
                        isHashorAt = false;
                    }
                    if(DictionaryContains || isHashorAt ){
                        res.add(tokens[i]);
                        retVal+= tokens[i] + " ";
                    }
                }
			}
		}
		return retVal;
	}
	
	public static String SCleaner(String data) {
		data = data.toLowerCase();
		String retVal="";
		for(int i = 0; i < data.length(); i++) {
			char ch = data.charAt(i);
			if(Character.isLetter(ch) || Character.isWhitespace(ch) || Character.toString(ch).equals("@") || 
					Character.toString(ch).equals("#")) {
				retVal += String.valueOf(ch);
			}else {
				retVal += " ";
			}
		}
		retVal  = WordString(retVal);
		return retVal;
	}
	
	public static ArrayList<String> TwoWords(String sentence){
		ArrayList<String> res = new ArrayList<>();
		ArrayList<String> container = new ArrayList<>();
		StringTokenizer str_tok = new StringTokenizer(sentence);
		
		while(str_tok.hasMoreTokens()) {
			container.add(str_tok.nextToken());
		}
		Collections.sort(container);
		
		for(int i = 0; i < container.size(); i++) {
			for(int j = (i+1); j<container.size(); j++) {
				String unify = (container.get(i)+" "+ container.get(j));
				if(!res.contains(unify) && !container.get(i).equals(container.get(j))) {
					res.add(unify);
				}
			}
		}
		return res;
		
	}
	
	public static ArrayList<String> ThreeWords(String sentence){
		ArrayList<String> res = new ArrayList<>();
		ArrayList<String> container = new ArrayList<>();
		StringTokenizer str_tok = new StringTokenizer(sentence);
		
		while(str_tok.hasMoreTokens()) {
			container.add(str_tok.nextToken());
		}
		Collections.sort(container);
		
		for(int i = 0; i < container.size(); i++) {
			for(int j = (i+1); j<container.size(); j++) {
				for(int k = (j+1); k<container.size(); k++) {
					String unify = (container.get(i)+" "+ container.get(j)+ " "+container.get(k));
					if(!res.contains(unify) && !container.get(i).equals(container.get(j)) 
							&& !container.get(i).equals(container.get(k)) && !container.get(j).equals(container.get(k)) ) {
						res.add(unify);
					}
					
				}
				
			}
		}
		return res;
		
	}
	
	public static void runPreproces(String[] args) {
		JobConf conf = new JobConf(Preproc.class);
        conf.setJobName("Preprocess");
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setNumReduceTasks(0);

        conf.setMapperClass(Preproc.Preprocessor.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public static void runFrequency(String[] args) {
		JobConf conf = new JobConf(Frequency.class);
        conf.setJobName("Frequencies");
        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        conf.setMapperClass(Frequency.Map.class);
        conf.setCombinerClass(Frequency.Reduce.class);
        conf.setReducerClass(Frequency.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
}

package au.edu.unsw.cse.bdmc.wordcount;
import java.util.Arrays;
import java.util.*;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{ //Input to mapper in form of (Object, Text), Output is (Text, Text)

    private Text word = new Text(); //Key for output from mapper
    
    public void map(Object key, Text value, Context context //Map logic. 
                    ) throws IOException, InterruptedException {
      String[] result = value.toString().split(" "); //Splitting the string. 
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); //Getting the file name. 
	  Configuration conf = context.getConfiguration();
	  String param = conf.get("test"); //Getting the variable From terminal
	  Integer ngrams = Integer.parseInt(param);
      for (int x=0; x<=result.length-ngrams; x++) {//LOOP THROUGH ALL THE WORDS

    	  if (result[x].length() != 0) {
    		  String temp = "";
    		  int k = x;
    		  while(k < x + ngrams ) {//Taking out the number of words required.
    			  temp = temp + result[k];
  				  temp += " ";
  				  k++;
    		  }

    		  word.set(temp);//Setting the Key.
    		  Text word_1 = new Text();
    		  word_1.set("1 "+fileName ); //Setting the value which is "1 filename"
    	  	  context.write(word, word_1);
    	  	  
    	  }
      }
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {//Input to reducer (Text, Text) output is also (Text, Text) 
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0; //Summing the occurrence 
      Configuration conf = context.getConfiguration();
	  String max_app = conf.get("app"); //Getting minimum count that has to be shown in the output file. 
	  Integer app = Integer.parseInt(max_app);
	  Iterator<Text> ite = values.iterator();
	  String[] files_list = new String[20]; //List for storing the file names
	  int l = 0;
	  while (ite.hasNext()) { //Tokenising all the strings. 
		  String lol = ite.next().toString(); 
		  String[] list = lol.split(" ");
		  sum += Integer.parseInt(list[0]); //Adding the first element which is the occurrences
		  files_list[l] = list[1]; //Adding the files to a filelist.
		  l++;
	  }
	  
	  l = 0;
	  Set<String> myset = new HashSet<String>(); //Hash set for removing duplicates
	  Collections.addAll(myset, files_list); //Adding the filelist
	  String main_files = ""; //String of ll the main files. 
	  for(String f: myset) { //looping through all the file names in the hash set. 
		  if (f != null) {
			  System.out.print(f);
			  main_files += f; //Adding the file names to the main file string.
			  main_files+= " ";
		  }
		  
	  }
	  System.out.println();
	  
	  String outputStr = Integer.toString(sum) + "\t" + main_files; //Concat of the Sum and the file names 
	  Text output = new Text();
	  output.set(outputStr);	  
      if (sum >= app) { //Checking if more than minimum number of appearances
    	  context.write(key, output); //Writing in the output. 
      }
    }
  }
  //Driver Function.
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("test", args[0]);
    conf.set("app", args[1]);
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

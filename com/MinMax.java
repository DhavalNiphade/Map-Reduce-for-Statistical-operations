package com;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;
import java.io.File;
import static java.lang.Math.sqrt;
import static java.lang.Math.abs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MinMax {

  public static class Map 
            extends Mapper<LongWritable, Text, Text, FloatWritable>{

    private final static FloatWritable one = new FloatWritable(); // type of output value
    private Text word = new Text("a");   // type of output key
    //word.set("a"); //Set the word to some arbitrary constant
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
     //StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token
    
      
      //while (itr.hasMoreTokens()) {	//Read file
        //word.set(itr.nextToken());    // set word as each input keyword
        context.write(word,new FloatWritable(Float.valueOf(value.toString())));     // create a pair <keyword, value read from file> 
      //}
    }
  }
  
  public static class Reduce
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	
    private FloatWritable result = new FloatWritable();
    
    
    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	
    float sum = 0; // initialize the sum for each keyword
    float min = 1, max=0, avg=0, temp=0, count=0;
    double dev=0, temp2=0;
		
	/*Print the hash map to the console
	for (IntWritable val : values) {
	String key = val.getKey().toString();
	Integer value = val.getValue();
	System.out.println("key, " + key + " value " + value);
      }
	Finish printing */


    for (FloatWritable val : values) {
      
	temp = val.get();
	sum += val.get();
	
	if(temp < min)
		min = temp;
		
	if(temp > max)
		max = temp;
		
	count++;
    }
    
    avg=sum/count;
       
    for (FloatWritable x : y) {
        temp = x.get();
    	temp2+=Math.pow((temp-avg), 2);
    	System.out.println(temp2);
      }
       
    dev=Math.sqrt(temp2);
    	
    
   System.out.println("Sum = "+sum);
   System.out.println("Count = "+count);
   System.out.println("Min = "+min);
   System.out.println("Max = "+max);
   System.out.println("Average = "+avg);
   System.out.println("Standard Deviation = "+dev);
     
      result.set(min);
      key.set("Min");
      context.write(key, result); // create a pair <keyword, number of occurrences>
      
      result.set(max);
      key.set("Max");
      context.write(key, result); // create a pair <keyword, number of occurrences>
      
      result.set(avg);
      key.set("Average");
      context.write(key, result); // create a pair <keyword, number of occurrences>
      
      
    }
  }

  // Driver program
  public static void main(String[] args) throws Exception {
	  
	// To delete output folder automatically - Prateek's code
	  File file = new File("/root/MoocHomeworks/MinMax/output");
	  if (file.isDirectory()) {
	  if (file.list().length != 0) {
	  String files[] = file.list();
	  for (String temp : files) {
	  // construct the file structure
	  File fileDelete = new File(file, temp);
	  fileDelete.delete();
	  }
	  }
	  if (file.list().length == 0) {
	  file.delete();
	  } else {
	  System.out.println("cannot delete folder");
	  }
	  }
	  else {
	  System.out.println("No output directory exist");
	  } 
	//
	   
    Configuration conf = new Configuration(); // configure the hdfs
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    if (otherArgs.length != 2) {
      System.err.println("Usage: MinMax <in> <out>");
      System.exit(2);
    } //control block to exit if more than or less than 2 arguments 

    // create a job with name "wordcount"
    Job job = new Job(conf, "minmax");
    job.setJarByClass(MinMax.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
   
    // Add a combiner here, not required to successfully run the wordcount program  

    // set output key type   
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(FloatWritable.class);
    //set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

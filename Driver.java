package com.stdatalabs.MRAadhaarAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver{
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		
		Job stateWiseCount = Job.getInstance(conf);
		stateWiseCount.setJobName("Aadhaar Data Analysis");
		stateWiseCount.setJarByClass(Driver.class);
		stateWiseCount.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		stateWiseCount.setMapperClass(NumUIDMapper.class);
		stateWiseCount.setReducerClass(NumUIDReducer.class);
		stateWiseCount.setInputFormatClass(TextInputFormat.class);
		stateWiseCount.setMapOutputKeyClass(Text.class);
		stateWiseCount.setMapOutputValueClass(IntWritable.class);
		stateWiseCount.setOutputKeyClass(Text.class);
		stateWiseCount.setOutputValueClass(Text.class);
		Path inputFilePath = new Path("/user/rahul/aadhardata/UIDAI-ENR-DETAIL-20170308.csv");
		Path outputFilePath = new Path("/user/rahul/aadhardata/statewise");
		FileInputFormat.addInputPath(stateWiseCount, inputFilePath);
		FileOutputFormat.setOutputPath(stateWiseCount, outputFilePath);
		FileSystem fs = FileSystem.newInstance(conf);
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		stateWiseCount.waitForCompletion(true);
			
	}
}

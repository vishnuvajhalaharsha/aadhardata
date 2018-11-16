package com.stdatalabs.MRAadhaarAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumUIDReducer extends
		Reducer<Text, IntWritable, Text, Text> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		int rejected = 0;
		for (IntWritable count : values) {
			if(count.get() == 0) {
				rejected = rejected+1;
			}
			sum += count.get();
		}
		//here sum is the count of approved and rejected is the count of rejected aadhars
		context.write(key, new Text(sum+"|"+rejected));
	}
}

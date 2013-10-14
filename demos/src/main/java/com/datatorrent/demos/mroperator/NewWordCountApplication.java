package com.datatorrent.demos.mroperator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;

public class NewWordCountApplication extends MapReduceApplication<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void conf() {
		setMapClass(WordCount.Map.class);
		setReduceClass(WordCount.Reduce.class);
		setCombineClass(WordCount.Reduce.class);
		setInputFormat(TextInputFormat.class);
		
	}
	
}

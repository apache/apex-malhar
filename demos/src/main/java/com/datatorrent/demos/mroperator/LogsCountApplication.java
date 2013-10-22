package com.datatorrent.demos.mroperator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;

public class LogsCountApplication extends MapReduceApplication<LongWritable, Text, DateWritable, IntWritable>
{

  @Override
  public void conf()
  {
    setMapClass(LogCountsPerHour.LogMapClass.class);
    // setCombineClass(LogCountsPerHour.LogReduce.class);
    setReduceClass(LogCountsPerHour.LogReduce.class);
    setInputFormat(TextInputFormat.class);

  }

}

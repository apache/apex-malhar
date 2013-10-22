package com.datatorrent.demos.mroperator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;

public class InvertedIndexApplication extends MapReduceApplication<LongWritable, Text, Text, Text>
{

  @Override
  public void conf()
  {
    setMapClass(LineIndexer.LineIndexMapper.class);
    setReduceClass(LineIndexer.LineIndexReducer.class);

    setInputFormat(TextInputFormat.class);

  }

}

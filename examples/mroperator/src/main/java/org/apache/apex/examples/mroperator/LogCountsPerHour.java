/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.mroperator;

import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * <p>LogCountsPerHour class.</p>
 *
 * @since 0.9.0
 */
public class LogCountsPerHour extends Configured implements Tool
{

  public static class LogMapClass extends MapReduceBase
      implements Mapper<LongWritable, Text, DateWritable, IntWritable>
  {
    private DateWritable date = new DateWritable();
    private static final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<DateWritable, IntWritable> output, Reporter reporter) throws IOException
    {
      // Get the value as a String; it is of the format:
      // 111.111.111.111 - - [16/Dec/2012:05:32:50 -0500] "GET / HTTP/1.1" 200 14791 "-" "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"
      String text = value.toString();

      // Get the date and time
      int openBracket = text.indexOf('[');
      int closeBracket = text.indexOf(']');
      if (openBracket != -1 && closeBracket != -1) {
        // Read the date
        String dateString = text.substring(text.indexOf('[') + 1, text.indexOf(']'));

        // Build a date object from a string of the form: 16/Dec/2012:05:32:50 -0500
        int index = 0;
        int nextIndex = dateString.indexOf('/');
        int day = Integer.parseInt(dateString.substring(index, nextIndex));

        index = nextIndex;
        nextIndex = dateString.indexOf('/', index + 1);
        String month = dateString.substring(index + 1, nextIndex);

        index = nextIndex;
        nextIndex = dateString.indexOf(':', index);
        int year = Integer.parseInt(dateString.substring(index + 1, nextIndex));

        index = nextIndex;
        nextIndex = dateString.indexOf(':', index + 1);
        int hour = Integer.parseInt(dateString.substring(index + 1, nextIndex));

        // Build a calendar object for this date
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DATE, day);
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.HOUR, hour);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        if (month.equalsIgnoreCase("dec")) {
          calendar.set(Calendar.MONTH, Calendar.DECEMBER);
        } else if (month.equalsIgnoreCase("nov")) {
          calendar.set(Calendar.MONTH, Calendar.NOVEMBER);
        } else if (month.equalsIgnoreCase("oct")) {
          calendar.set(Calendar.MONTH, Calendar.OCTOBER);
        } else if (month.equalsIgnoreCase("sep")) {
          calendar.set(Calendar.MONTH, Calendar.SEPTEMBER);
        } else if (month.equalsIgnoreCase("aug")) {
          calendar.set(Calendar.MONTH, Calendar.AUGUST);
        } else if (month.equalsIgnoreCase("jul")) {
          calendar.set(Calendar.MONTH, Calendar.JULY);
        } else if (month.equalsIgnoreCase("jun")) {
          calendar.set(Calendar.MONTH, Calendar.JUNE);
        } else if (month.equalsIgnoreCase("may")) {
          calendar.set(Calendar.MONTH, Calendar.MAY);
        } else if (month.equalsIgnoreCase("apr")) {
          calendar.set(Calendar.MONTH, Calendar.APRIL);
        } else if (month.equalsIgnoreCase("mar")) {
          calendar.set(Calendar.MONTH, Calendar.MARCH);
        } else if (month.equalsIgnoreCase("feb")) {
          calendar.set(Calendar.MONTH, Calendar.FEBRUARY);
        } else if (month.equalsIgnoreCase("jan")) {
          calendar.set(Calendar.MONTH, Calendar.JANUARY);
        }


        // Output the date as the key and 1 as the value
        date.setDate(calendar.getTime());
        output.collect(date, one);
      }
    }
  }

  public static class LogReduce extends MapReduceBase
      implements Reducer<DateWritable, IntWritable, DateWritable, IntWritable>
  {
    public void reduce(DateWritable key, Iterator<IntWritable> values, OutputCollector<DateWritable, IntWritable> output, Reporter reporter) throws IOException
    {
      // Iterate over all of the values (counts of occurrences of this word)
      int count = 0;
      while (values.hasNext()) {
        // Add the value to our count
        count += values.next().get();
      }

      // Output the word with its count (wrapped in an IntWritable)
      output.collect(key, new IntWritable(count));
    }
  }


  public int run(String[] args) throws Exception
  {
    // Create a configuration
    Configuration conf = getConf();

    // Create a job from the default configuration that will use the WordCount class
    JobConf job = new JobConf(conf, LogCountsPerHour.class);

    // Define our input path as the first command line argument and our output path as the second
    Path in = new Path(args[0]);
    Path out = new Path(args[1]);

    // Create File Input/Output formats for these paths (in the job)
    FileInputFormat.setInputPaths(job, in);
    FileOutputFormat.setOutputPath(job, out);

    // Configure the job: name, mapper, reducer, and combiner
    job.setJobName("LogAveragePerHour");
    job.setMapperClass(LogMapClass.class);
    job.setReducerClass(LogReduce.class);
    job.setCombinerClass(LogReduce.class);

    // Configure the output
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(DateWritable.class);
    job.setOutputValueClass(IntWritable.class);

    // Run the job
    JobClient.runJob(job);
    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    // Start the LogCountsPerHour MapReduce application
    int res = ToolRunner.run(new Configuration(), new LogCountsPerHour(), args);
    System.exit(res);
  }
}

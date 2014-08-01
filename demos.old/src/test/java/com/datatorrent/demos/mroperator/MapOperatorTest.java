/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.mroperator;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.demos.mroperator.MapOperator;
import com.datatorrent.demos.mroperator.WordCount;
import com.datatorrent.lib.testbench.CollectorTestSink;


@SuppressWarnings("deprecation")
public class MapOperatorTest
{

  private static Logger logger = LoggerFactory.getLogger(MapOperatorTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MapOperator<LongWritable, Text, Text, IntWritable>());
  }

  public void testNodeProcessingSchema(MapOperator<LongWritable, Text, Text, IntWritable> oper) throws IOException
  {

    CollectorTestSink sortSink = new CollectorTestSink();
    oper.output.setSink(sortSink);

    oper.setMapClass(WordCount.Map.class);
    oper.setCombineClass(WordCount.Reduce.class);
    oper.setDirName("src/test/resources/mroperator/");
    oper.setConfigFile(null);
    oper.setInputFormatClass(TextInputFormat.class);

    Configuration conf = new Configuration();
    JobConf jobConf = new JobConf(conf);
    FileInputFormat.setInputPaths(jobConf, new Path("src/test/resources/mroperator/"));
    TextInputFormat inputFormat = new TextInputFormat();
    inputFormat.configure(jobConf);
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    Serializer keySerializer = serializationFactory.getSerializer(splits[0].getClass());
    keySerializer.open(oper.getOutstream());
    keySerializer.serialize(splits[0]);
    oper.setInputSplitClass(splits[0].getClass());
    keySerializer.close();
    oper.setup(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();
    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 6, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      logger.debug(o.toString());
    }
    logger.debug("Done testing round\n");
  }


}

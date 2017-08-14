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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.io.FileUtils;
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

@SuppressWarnings("deprecation")
public class MapOperatorTest
{

  private static Logger LOG = LoggerFactory.getLogger(MapOperatorTest.class);

  @Rule
  public TestMeta testMeta = new TestMeta();
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
    oper.setDirName(testMeta.testDir);
    oper.setConfigFile(null);
    oper.setInputFormatClass(TextInputFormat.class);

    Configuration conf = new Configuration();
    JobConf jobConf = new JobConf(conf);
    FileInputFormat.setInputPaths(jobConf, new Path(testMeta.testDir));
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

    Assert.assertEquals("number emitted tuples", 3, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      LOG.debug(o.toString());
    }
    LOG.debug("Done testing round\n");
    oper.teardown();
  }

  public static class TestMeta extends TestWatcher
  {
    public final String file1 = "file1";
    public String baseDir;
    public String testDir;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      baseDir = "target/" + className;
      testDir = baseDir + "/" + methodName;
      try {
        FileUtils.forceMkdir(new File(testDir));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      createFile(testDir + "/" + file1, "1\n2\n3\n1\n2\n3\n");
    }

    private void createFile(String fileName, String data)
    {
      BufferedWriter output = null;
      try {
        output = new BufferedWriter(new FileWriter(new File(fileName)));
        output.write(data);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } finally {
        if (output != null) {
          try {
            output.close();
          } catch (IOException ex) {
            LOG.error("not able to close the output stream: ", ex);
          }
        }
      }
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File(baseDir));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

}

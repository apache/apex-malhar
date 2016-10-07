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
package org.apache.apex.malhar.sketches;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class QuantilesEstimatorTest
{
  private static Logger LOG = LoggerFactory.getLogger(QuantilesEstimatorTest.class);

  @Test
  public void testQuantiles()
  {
    QuantilesEstimator quantilesOp = new QuantilesEstimator(128, (short)12345);

    CollectorTestSink<double[]> sink = new CollectorTestSink<>();
    TestUtils.setSink(quantilesOp.quantilesOutput, sink);

    Random rand = new Random(1234L);
    ArrayList<Double> randArray = new ArrayList<>();

    quantilesOp.setup(null);
    quantilesOp.beginWindow(0);

    for (int i = 0; i < 101; i++) {
      double r = rand.nextGaussian();
      quantilesOp.data.process(r);
      randArray.add(r);
    }

    quantilesOp.endWindow();

    Collections.sort(randArray);

    Assert.assertEquals("Captures all computed quantiles", sink.collectedTuples.size(), 101);
    Assert.assertTrue("Computes median correctly", randArray.get(50) == sink.collectedTuples.get(100)[2]);
  }

  @Test
  public void testCDF()
  {
    QuantilesEstimator quantilesOp = new QuantilesEstimator(128, (short)12345);

    CollectorTestSink<Double> sink = new CollectorTestSink<>();
    TestUtils.setSink(quantilesOp.cdfOutput, sink);

    quantilesOp.setup(null);
    quantilesOp.beginWindow(0);

    for (int i = 0; i < 1001; i++) {
      double r = 0.001 * i;
      quantilesOp.data.process(r);
    }
    quantilesOp.endWindow();

    List<Double> cdfValues = sink.collectedTuples;
    Assert.assertTrue("Highest CDF value is approx 1.0", cdfValues.get(cdfValues.size() - 1) >= 0.99 &&
        cdfValues.get(cdfValues.size() - 1) <= 1.0);
    Assert.assertTrue("Lowest CDF value is approx 0.0", cdfValues.get(0) >= 0.0 &&
        cdfValues.get(0) <= 0.01);
  }

  @Test
  public void testPMF()
  {
    QuantilesEstimator quantilesOp = new QuantilesEstimator(128, (short)12345);
    double[] intervals = {0.0, 0.20, 0.40, 0.60, 0.80, 1.0};
    quantilesOp.setPmfIntervals(intervals);

    CollectorTestSink<double[]> sink = new CollectorTestSink<>();
    TestUtils.setSink(quantilesOp.pmfOutput, sink);

    quantilesOp.setup(null);
    quantilesOp.beginWindow(0);
    for (int i = 0; i < 1000; i++) {
      quantilesOp.data.process(0.001 * i);
    }
    quantilesOp.endWindow();

    double[] finalPmf = sink.collectedTuples.get(sink.collectedTuples.size() - 1);
    Assert.assertTrue("Probability Mass between 0.0 and 0.2 is approx 0.2", finalPmf[1] >= 0.19 && finalPmf[1] <= 0.21);
  }

  public static class Application implements StreamingApplication
  {
    private String outputFileName;

    public static class NumberSource extends BaseOperator implements InputOperator
    {
      public final DefaultOutputPort<Double> output = new DefaultOutputPort<>();

      private Random rand = new Random(1234L);

      public NumberSource() {}

      @Override
      public void emitTuples()
      {
        output.emit(rand.nextGaussian());
      }
    }

    public static class QuantilesWriter extends BaseOperator
    {
      private String outputFileName;
      private transient PrintStream outputStream;

      public final transient DefaultInputPort<double[]> inputPort = new DefaultInputPort<double[]>()
      {
        @Override
        public void process(double[] tuple)
        {
          if (outputStream != null) {
            double mean = tuple[2];
            outputStream.println(mean);
          }
        }
      };

      @Override
      public void setup(Context.OperatorContext context)
      {
        if (outputFileName != null) {
          try {
            outputStream = new PrintStream(new FileOutputStream(outputFileName), true);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public void teardown()
      {
        outputStream.close();
      }

      public void setOutputFileName(String fileName)
      {
        this.outputFileName = fileName;
      }
    }

    public void setOutputFileName(String fileName)
    {
      this.outputFileName = fileName;
    }

    public void populateDAG(DAG dag, Configuration conf)
    {
      NumberSource source = dag.addOperator("source", NumberSource.class);
      QuantilesEstimator quantilesOp = dag.addOperator("quantilesEstimator", QuantilesEstimator.class);
      QuantilesWriter writer = new QuantilesWriter();
      writer.setOutputFileName(outputFileName);
      dag.addOperator("fileWriter", writer);

      dag.addStream("random number stream", source.output, quantilesOp.data);
      dag.addStream("quantiles stream", quantilesOp.quantilesOutput, writer.inputPort);
    }
  }

  @Test
  public void testInDAG() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    Application testApp = new Application();
    String outputFileName = "target/output.txt";
    long timeout = 2000; // 2 seconds

    new File(outputFileName).delete();
    testApp.setOutputFileName(outputFileName);
    lma.prepareDAG(testApp, conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    File target = new File("target/output.txt");
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime <= timeout) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        break;
      }
      if (target.length() > 100000) {
        break;
      }
    }

    Assert.assertTrue(target.exists());
    try {
      List<String> lines = FileUtils.readLines(target);
      double finalMedian = Double.parseDouble(lines.get(lines.size() - 1));
      Assert.assertTrue("Median is close to 0.0", -0.05 <= finalMedian && finalMedian <= 0.05);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}

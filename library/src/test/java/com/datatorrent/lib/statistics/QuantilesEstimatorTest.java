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
package com.datatorrent.lib.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class QuantilesEstimatorTest {

  @Test
  public void testQuantiles() {
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
  public void testCDF() {
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
  public void testPMF() {
    QuantilesEstimator quantilesOp = new QuantilesEstimator(128, (short)12345);
    double[] intervals = {0.0, 0.20, 0.40, 0.60, 0.80, 1.0} ;
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

  @Test
  public void testQuantilesSwitch() {
    QuantilesEstimator quantilesOp = new QuantilesEstimator(128, (short)12345);
    quantilesOp.setComputeQuantiles(false);

    CollectorTestSink<Double> sink = new CollectorTestSink<>();
    TestUtils.setSink(quantilesOp.quantilesOutput, sink);

    Random rand = new Random();

    quantilesOp.setup(null);
    quantilesOp.beginWindow(0);

    for (int i = 0; i < 10; i++) {
      quantilesOp.data.process(rand.nextGaussian());
    }
    quantilesOp.endWindow();

    Assert.assertTrue("No tuples emitted from quantiles output port", sink.collectedTuples.size() == 0);
  }

  @Test
  public void testCdfSwitch() {
    QuantilesEstimator quantilesOp = new QuantilesEstimator(128, (short)12345);
    quantilesOp.setComputeCdf(false);

    CollectorTestSink<Double> sink = new CollectorTestSink<>();
    TestUtils.setSink(quantilesOp.cdfOutput, sink);

    Random rand = new Random();

    quantilesOp.setup(null);
    quantilesOp.beginWindow(0);

    for (int i = 0; i < 10; i++) {
      quantilesOp.data.process(rand.nextGaussian());
    }
    quantilesOp.endWindow();

    Assert.assertTrue("No tuples emitted from cdf output port", sink.collectedTuples.size() == 0);
  }

  @Test
  public void testPmfSwitch() {
    QuantilesEstimator quantilesOp = new QuantilesEstimator(128, (short)12345);
    quantilesOp.setComputePmf(false);

    CollectorTestSink<Double> sink = new CollectorTestSink<>();
    TestUtils.setSink(quantilesOp.pmfOutput, sink);

    Random rand = new Random();

    quantilesOp.setup(null);
    quantilesOp.beginWindow(0);

    for (int i = 0; i < 10; i++) {
      quantilesOp.data.process(rand.nextGaussian());
    }
    quantilesOp.endWindow();

    Assert.assertTrue("No tuples emitted from pmf output port", sink.collectedTuples.size() == 0);
  }
}

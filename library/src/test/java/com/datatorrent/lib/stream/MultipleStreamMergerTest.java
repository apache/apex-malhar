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
package com.datatorrent.lib.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.testbench.RandomWordGenerator;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class MultipleStreamMergerTest
{
  private static Logger LOG = LoggerFactory.getLogger(MultipleStreamMergerTest.class);

  private ArrayList<MultipleStreamMerger<byte[]>.Stream> streamsToAddToDag;
  private ArrayList<MultipleStreamMerger<byte[]>.NamedMerger> operatorsToAdd;

  private static Counter counterOp = new Counter();

  private static int tuplesToSend = 173;
  private static int streamsToMerge = 15;

  private static Map<Long, AtomicLong> tuplesReceived = new HashMap<>();

  @Before
  public void setUp() throws Exception
  {
    streamsToAddToDag = new ArrayList<>();
    operatorsToAdd = new ArrayList<>();
  }

  @Test
  public void mergeTwoStreams()
  {
    RandomWordGenerator randomWordGenerator = new RandomWordGenerator();
    RandomWordGenerator randomWordGenerator2 = new RandomWordGenerator();

    randomWordGenerator.setTuplesPerWindow(1);
    randomWordGenerator2.setTuplesPerWindow(1);

    MultipleStreamMerger<byte[]> merger = new MultipleStreamMerger<>();
    merger.merge(randomWordGenerator.output)
        .merge(randomWordGenerator2.output);

    merger.constructMergeTree(streamsToAddToDag, operatorsToAdd);

    assertEquals("Count of created streams", 2, streamsToAddToDag.size());
    assertEquals("Count of created operators", 1, operatorsToAdd.size());

    // Next check actual connections
    assertEquals("Generator 1 stream", randomWordGenerator.output,
        streamsToAddToDag.get(0).sourcePort.port);

    assertEquals("Generator 2 stream", randomWordGenerator2.output,
        streamsToAddToDag.get(1).sourcePort.port);

    assertEquals("Final operator input_1", operatorsToAdd.get(0).merger.data1, streamsToAddToDag.get(0).destPort);
    assertEquals("Final operator input_2", operatorsToAdd.get(0).merger.data2, streamsToAddToDag.get(1).destPort);
  }

  @Test(expected = IllegalArgumentException.class)
  public void mergeOneStream()
  {
    RandomWordGenerator randomWordGenerator = new RandomWordGenerator();
    MultipleStreamMerger<byte[]> merger = new MultipleStreamMerger<>();
    merger.merge(randomWordGenerator.output);
    merger.constructMergeTree(streamsToAddToDag, operatorsToAdd);
  }

  @Test(expected = IllegalArgumentException.class)
  public void mergeZeroStream()
  {
    MultipleStreamMerger<byte[]> merger = new MultipleStreamMerger<>();
    merger.constructMergeTree(streamsToAddToDag, operatorsToAdd);
  }

  public static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.debug("Application - PopulateDAG");
      NumGenerator[] generators = new NumGenerator[streamsToMerge];
      MultipleStreamMerger<Long> merger = new MultipleStreamMerger<>();

      for (int i = 0; i < streamsToMerge; i++) {
        generators[i] = new NumGenerator();
        dag.addOperator("Generator " + i, generators[i]);
        merger.merge(generators[i].out);
      }

      DefaultOutputPort streamOutput = merger.mergeStreams(dag, conf);

      // Count the number of tuples in the merger
      dag.addOperator("Counter", counterOp);

      // And then we should see the output
      dag.addStream("merger-counter", streamOutput, counterOp.counter);
    }
  }

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync(); // runs for 10 seconds and quits

      long startTime = System.currentTimeMillis();
      long timeout = 5 * 1000; //5 seconds
      while (System.currentTimeMillis() - startTime < timeout && counterOp.getCount() < streamsToMerge * tuplesToSend) {
        LOG.info("Sleeping....");

        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          break;
        }
      }

      assertEquals("Total sent not merged correctly.", streamsToMerge * tuplesToSend, counterOp.getCount());
      // Check that all tuples were received that were sent

      for (long i = 0; i < tuplesToSend; i++) {
        assertTrue("Received count not null.", tuplesReceived.get(i) != null);
        assertEquals("Received count matches sent count for " + i, streamsToMerge, tuplesReceived.get(i).longValue());
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private static class NumGenerator extends BaseOperator implements InputOperator
  {
    private long count = 0;

    public transient DefaultOutputPort<Long> out = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      if (count < tuplesToSend) {
        out.emit(count++);
      }
    }
  }

  private static class Counter extends BaseOperator
  {
    private static long count = 0;

    public final transient DefaultInputPort<Long> counter = new DefaultInputPort<Long>()
    {
      @Override
      public void process(Long tuple)
      {
        if (tuplesReceived.containsKey(tuple)) {
          tuplesReceived.get(tuple).getAndIncrement();
        } else {
          tuplesReceived.put(tuple, new AtomicLong(1));
        }
        count++;
      }
    };

    public long getCount()
    {
      return count;
    }
  }
}

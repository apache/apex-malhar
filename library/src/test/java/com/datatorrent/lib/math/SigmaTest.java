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
package com.datatorrent.lib.math;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.testbench.SumTestSink;

import static org.junit.Assert.assertEquals;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.Sigma}
 * <p>
 * 
 */

public class SigmaTest
{
  private static int tuplesToSend = 173;
  private static int numPartitions = 15;
  private static Counter checker = new Counter();

  /**
   * Test oper logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeSchemaProcessing()
  {
    Sigma oper = new Sigma();
    SumTestSink lmultSink = new SumTestSink();
    SumTestSink imultSink = new SumTestSink();
    SumTestSink dmultSink = new SumTestSink();
    SumTestSink fmultSink = new SumTestSink();
    oper.longResult.setSink(lmultSink);
    oper.integerResult.setSink(imultSink);
    oper.doubleResult.setSink(dmultSink);
    oper.floatResult.setSink(fmultSink);

    int sum = 0;
    ArrayList<Integer> list = new ArrayList<Integer>();
    for (int i = 0; i < 100; i++) {
      list.add(i);
      sum += i;
    }

    oper.beginWindow(0); //
    oper.input.process(list);
    oper.endWindow(); //

    oper.beginWindow(1); //
    oper.input.process(list);
    oper.endWindow(); //
    sum = sum * 2;

    Assert.assertEquals("sum was", sum, lmultSink.val.intValue());
    Assert.assertEquals("sum was", sum, imultSink.val.intValue());
    Assert.assertEquals("sum was", sum, dmultSink.val.intValue());
    Assert.assertEquals("sum", sum, fmultSink.val.intValue());
  }

  public static class Application implements StreamingApplication
  {
    private static Logger LOG = LoggerFactory.getLogger(Application.class);

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.debug("Application - PopulateDAG");
      NumGenerator generator = new NumGenerator();
      Sigma<Long> summer = new Sigma<>();

      dag.addOperator("Generator.", generator);
      dag.addOperator("Summer", summer);
      dag.addOperator("Checker", checker);

      StatelessPartitioner<Sigma> partitioner = new StatelessPartitioner<>();
      partitioner.setPartitionCount(numPartitions);
      dag.setAttribute(summer, Context.OperatorContext.PARTITIONER, partitioner);

      // And then we should see the output
      dag.addStream("generator-summer", generator.out, summer.input);
      dag.addStream("summer-checker", summer.longResult, checker.counter);
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
      lc.run(15000); // runs for 10 seconds and quits

      assertEquals("Total sum", tuplesToSend*numPartitions, checker.getCount());
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private static class NumGenerator extends BaseOperator implements InputOperator
  {
    private long count = 0;

    public transient DefaultOutputPort<List<Long>> out = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      if (count < tuplesToSend) {
        LinkedList<Long> list = new LinkedList<>();
        list.add(1l);
        out.emit(list);
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
        count += tuple;
      }
    };

    public long getCount()
    {
      return count;
    }
  }
}


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
package org.apache.apex.malhar.lib.dedup;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;

/**
 * Tests whether the operator functions correctly when partitioned
 * The partitioning in Dedup is overridden by partitioning on basis of the key in the tuple.
 *
 */
public class DeduperPartitioningTest
{
  public static final int NUM_DEDUP_PARTITIONS = 5;
  private static boolean testFailed = false;

  /**
   * Application to test the partitioning
   *
   */
  public static class TestDedupApp implements StreamingApplication
  {
    TestDeduper dedup;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      TestGenerator gen = dag.addOperator("Generator", new TestGenerator());

      dedup = dag.addOperator("Deduper", new TestDeduper());
      dedup.setKeyExpression("id");
      dedup.setTimeExpression("eventTime.getTime()");
      dedup.setBucketSpan(60);
      dedup.setExpireBefore(600);

      ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());
      dag.addStream("Generator to Dedup", gen.output, dedup.input);
      dag.addStream("Dedup to Console", dedup.unique, console.input);
      dag.setInputPortAttribute(dedup.input, Context.PortContext.TUPLE_CLASS, TestEvent.class);
      dag.setOutputPortAttribute(dedup.unique, Context.PortContext.TUPLE_CLASS, TestEvent.class);
      dag.setAttribute(dedup, Context.OperatorContext.PARTITIONER,
          new StatelessPartitioner<TimeBasedDedupOperator>(NUM_DEDUP_PARTITIONS));
    }
  }

  public static class TestDeduper extends TimeBasedDedupOperator implements StatsListener
  {
    int operatorId;
    HashMap<Integer, Integer> partitionMap = Maps.newHashMap();
    transient CountDownLatch latch = new CountDownLatch(1);
    int tuplesProcessed = 0;
    @AutoMetric
    int tuplesProcessedCompletely = 0;

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      operatorId = context.getId();
    }

    @Override
    protected void processTuple(Object tuple)
    {
      TestEvent event = (TestEvent)tuple;
      if (partitionMap.containsKey(event.id)) {
        if (partitionMap.get(event.id) != operatorId) {
          testFailed = true;
          throw new RuntimeException("Wrong tuple assignment");
        }
      } else {
        partitionMap.put(event.id, operatorId);
      }
      tuplesProcessed++;
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      tuplesProcessedCompletely = tuplesProcessed;
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      Stats.OperatorStats operatorStats = stats.getLastWindowedStats().get(stats.getLastWindowedStats().size() - 1);
      tuplesProcessedCompletely = (Integer)operatorStats.metrics.get("tuplesProcessedCompletely");
      if (tuplesProcessedCompletely >= 1000) {
        latch.countDown();
      }
      return null;
    }
  }

  public static class TestGenerator extends BaseOperator implements InputOperator
  {

    public final transient DefaultOutputPort<TestEvent> output = new DefaultOutputPort<>();
    private final transient Random r = new Random();

    @Override
    public void emitTuples()
    {
      TestEvent event = new TestEvent();
      event.id = r.nextInt(100);
      output.emit(event);
    }
  }

  public static class TestEvent
  {
    private int id;
    private Date eventTime;

    public TestEvent()
    {
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public Date getEventTime()
    {
      return eventTime;
    }

    public void setEventTime(Date eventTime)
    {
      this.eventTime = eventTime;
    }
  }

  /**
   * This test validates whether a tuple key goes to exactly one partition
   */
  @Test
  public void testDeduperStreamCodec() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    TestDedupApp app = new TestDedupApp();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    app.dedup.latch.await();
    lc.shutdown();
    Assert.assertFalse(testFailed);
  }
}

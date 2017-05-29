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
package org.apache.apex.malhar.lib.join;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@Ignore
public class POJOPartitionJoinOperatorTest
{
  public static final int NUM_OF_PARTITIONS = 4;
  public static final int TOTAL_TUPLES_PROCESS = 1000;
  private static boolean testFailed = false;

  public static class PartitionTestJoinOperator extends POJOInnerJoinOperator implements StatsListener
  {
    public int operatorId;
    HashMap<Integer, Integer> partitionMap = Maps.newHashMap();
    transient CountDownLatch latch = new CountDownLatch(1);
    int tuplesProcessed = 0;
    @AutoMetric
    int tuplesProcessedCompletely = 0;

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      operatorId = context.getId();
    }

    @Override
    protected void processTuple(Object tuple, boolean isStream1Data)
    {
      // Verifying the data for stream1
      if (!isStream1Data) {
        return;
      }
      int key = (int)extractKey(tuple, isStream1Data);
      if (partitionMap.containsKey(key)) {
        if (partitionMap.get(key) != operatorId) {
          testFailed = true;
        }
      } else {
        partitionMap.put(key, operatorId);
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
    public StatsListener.Response processStats(StatsListener.BatchedOperatorStats stats)
    {
      Stats.OperatorStats operatorStats = stats.getLastWindowedStats().get(stats.getLastWindowedStats().size() - 1);
      tuplesProcessedCompletely = (Integer)operatorStats.metrics.get("tuplesProcessedCompletely");
      if (tuplesProcessedCompletely >= TOTAL_TUPLES_PROCESS) {
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
    public int id;
    public Date eventTime;

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

  public static class JoinApp implements StreamingApplication
  {
    public PartitionTestJoinOperator joinOp;

    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      TestGenerator gen1 = dag.addOperator("Generator1", new TestGenerator());
      TestGenerator gen2 = dag.addOperator("Generator2", new TestGenerator());

      joinOp = dag.addOperator("Join", new PartitionTestJoinOperator());
      joinOp.setLeftKeyExpression("id");
      joinOp.setRightKeyExpression("id");
      joinOp.setIncludeFieldStr("id,eventTime;id,eventTime");
      joinOp.setExpiryTime(10000L);

      ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

      dag.addStream("Gen1ToJoin", gen1.output, joinOp.input1);
      dag.addStream("Gen2ToJoin", gen2.output, joinOp.input2);
      dag.addStream("JoinToConsole", joinOp.outputPort, console.input);
      dag.setInputPortAttribute(joinOp.input1, DAG.InputPortMeta.TUPLE_CLASS,TestEvent.class);
      dag.setInputPortAttribute(joinOp.input2, DAG.InputPortMeta.TUPLE_CLASS,TestEvent.class);
      dag.setOutputPortAttribute(joinOp.outputPort, DAG.InputPortMeta.TUPLE_CLASS,TestEvent.class);
      dag.setAttribute(joinOp, Context.OperatorContext.PARTITIONER,
          new StatelessPartitioner<PartitionTestJoinOperator>(NUM_OF_PARTITIONS));
    }
  }

  /**
   * This test validates whether a tuple key goes to exactly one partition
   */
  @Test
  public void testJoinOpStreamCodec() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    JoinApp app = new JoinApp();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    app.joinOp.latch.await();
    lc.shutdown();
    Assert.assertFalse(testFailed);
  }

}

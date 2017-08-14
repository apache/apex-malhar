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

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.fileaccess.FileAccessFSImpl;
import org.apache.apex.malhar.lib.fileaccess.TFileImpl;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

public class DeduperOrderingTest
{
  public static boolean testFailed = false;

  @Test
  public void testApplication() throws IOException, Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    DeduperOrderingTestApp app = new DeduperOrderingTestApp();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    app.verifier.latch.await();
    Assert.assertFalse(testFailed);
    lc.shutdown();
  }

  public static class DeduperOrderingTestApp implements StreamingApplication
  {
    Verifier verifier;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomDedupDataGenerator random = dag.addOperator("Input", RandomDedupDataGenerator.class);

      TimeBasedDedupOperator dedup = dag.addOperator("Dedup", TimeBasedDedupOperator.class);
      dedup.setKeyExpression("key");
      dedup.setTimeExpression("date.getTime()");
      dedup.setBucketSpan(10);
      dedup.setExpireBefore(60);
      dedup.setPreserveTupleOrder(true);
      FileAccessFSImpl fAccessImpl = new TFileImpl.DTFileImpl();
      fAccessImpl.setBasePath(dag.getAttributes().get(DAG.APPLICATION_PATH) + "/bucket_data");
      dedup.managedState.setFileAccess(fAccessImpl);
      dag.setInputPortAttribute(dedup.input, Context.PortContext.TUPLE_CLASS, TestPojo.class);

      verifier = dag.addOperator("Verifier", Verifier.class);

      dag.addStream("Input to Dedup", random.output, dedup.input);
      dag.addStream("Dedup to Unique", dedup.unique, verifier.unique);
      dag.addStream("Dedup to Duplicate", dedup.duplicate, verifier.duplicate);
      dag.addStream("Dedup to Expired", dedup.expired, verifier.expired);
    }
  }

  public static class RandomDedupDataGenerator extends BaseOperator implements InputOperator
  {
    private final long count = 500;
    private long windowCount = 0;
    private long sequenceId = 0;

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

    @Override
    public void beginWindow(long windowId)
    {
      windowCount = 0;
    }

    @Override
    public void emitTuples()
    {
      if (windowCount < count) {
        TestPojo pojo = new TestPojo(sequenceId, new Date(), sequenceId);
        output.emit(pojo);
        sequenceId++;
        windowCount++;
      }
    }
  }

  public static class Verifier extends BaseOperator implements StatsListener
  {
    long prevSequence = 0;

    public transient CountDownLatch latch = new CountDownLatch(1);
    @AutoMetric
    int count = 0;
    public final transient DefaultInputPort<Object> unique = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        TestPojo pojo = (TestPojo)tuple;
        if (pojo.getSequence() < prevSequence) {
          testFailed = true;
        }
        Verifier.this.count++;
        prevSequence = pojo.sequence;
      }
    };

    public final transient DefaultInputPort<Object> duplicate = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        TestPojo pojo = (TestPojo)tuple;
        if (pojo.getSequence() < prevSequence) {
          testFailed = true;
        }
        Verifier.this.count++;
        prevSequence = pojo.sequence;
      }
    };

    public final transient DefaultInputPort<Object> expired = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        TestPojo pojo = (TestPojo)tuple;
        if (pojo.getSequence() < prevSequence) {
          testFailed = true;
        }
        Verifier.this.count++;
        prevSequence = pojo.sequence;
      }
    };

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      Stats.OperatorStats operatorStats = stats.getLastWindowedStats().get(stats.getLastWindowedStats().size() - 1);
      count = (Integer)operatorStats.metrics.get("count");
      if (count >= 1000) {
        latch.countDown();
      }
      return null;
    }
  }
}

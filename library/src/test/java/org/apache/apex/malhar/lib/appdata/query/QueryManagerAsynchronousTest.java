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
package org.apache.apex.malhar.lib.appdata.query;

import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.query.serde.MessageSerializerFactory;
import org.apache.apex.malhar.lib.appdata.schemas.ResultFormatter;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.api.DefaultOutputPort;

public class QueryManagerAsynchronousTest
{
  @Rule
  public TestWatcher testMeta = new InterruptClear();

  public static class InterruptClear extends TestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      Thread.interrupted();
    }

    @Override
    protected void finished(Description description)
    {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
        //noop
      }
      Thread.interrupted();
    }
  }

  @Test
  public void stressTest() throws Exception
  {
    final int totalTuples = 100000;
    final int batchSize = 100;
    final double waitMillisProb = .01;

    AppDataWindowEndQueueManager<MockQuery, Void> queueManager = new AppDataWindowEndQueueManager<MockQuery, Void>();

    DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();
    CollectorTestSink<MockResult> sink = new CollectorTestSink<MockResult>();
    TestUtils.setSink(outputPort, sink);

    MessageSerializerFactory msf = new MessageSerializerFactory(new ResultFormatter());

    QueryManagerAsynchronous<MockQuery, Void, MutableLong, MockResult> queryManagerAsynch =
        new QueryManagerAsynchronous<>(outputPort, queueManager, new NOPQueryExecutor(waitMillisProb), msf,
        Thread.currentThread());

    Thread producerThread = new Thread(new ProducerThread(queueManager, totalTuples, batchSize, waitMillisProb));
    producerThread.start();
    producerThread.setName("Producer Thread");

    long startTime = System.currentTimeMillis();

    queryManagerAsynch.setup(null);

    int numWindows = 0;

    for (; sink.collectedTuples.size() < totalTuples && ((System.currentTimeMillis() - startTime) < 60000);
        numWindows++) {
      queryManagerAsynch.beginWindow(numWindows);
      Thread.sleep(100);
      queryManagerAsynch.endWindow();
    }

    producerThread.stop();
    queryManagerAsynch.teardown();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      //Do Nothing
    }

    Assert.assertEquals(totalTuples, sink.collectedTuples.size());
  }

  public static class NOPQueryExecutor implements QueryExecutor<MockQuery, Void, MutableLong, MockResult>
  {
    private final double waitMillisProb;
    private final Random rand = new Random();

    public NOPQueryExecutor(double waitMillisProb)
    {
      this.waitMillisProb = waitMillisProb;
    }

    @Override
    public MockResult executeQuery(MockQuery query, Void metaQuery, MutableLong queueContext)
    {
      if (rand.nextDouble() < waitMillisProb) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }

      return new MockResult(query);
    }
  }

  public static class ProducerThread implements Runnable
  {
    private final int totalTuples;
    private final int batchSize;
    private final AppDataWindowEndQueueManager<MockQuery, Void> queueManager;
    private final double waitMillisProb;
    private final Random rand = new Random();

    public ProducerThread(AppDataWindowEndQueueManager<MockQuery, Void> queueManager, int totalTuples, int batchSize,
        double waitMillisProb)
    {
      this.queueManager = queueManager;
      this.totalTuples = totalTuples;
      this.batchSize = batchSize;
      this.waitMillisProb = waitMillisProb;
    }

    @Override
    public void run()
    {
      int numLoops = totalTuples / batchSize;

      for (int loopCounter = 0, tupleCounter = 0; loopCounter < numLoops; loopCounter++, tupleCounter++) {
        for (int batchCounter = 0; batchCounter < batchSize; batchCounter++, tupleCounter++) {
          queueManager.enqueue(new MockQuery(tupleCounter + ""), null, new MutableLong(1L));

          if (rand.nextDouble() < waitMillisProb) {
            try {
              Thread.sleep(1);
            } catch (InterruptedException ex) {
              throw new RuntimeException(ex);
            }
          }
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(QueryManagerAsynchronousTest.class);
}

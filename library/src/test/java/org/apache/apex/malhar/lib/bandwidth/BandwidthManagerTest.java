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

package org.apache.apex.malhar.lib.bandwidth;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class BandwidthManagerTest
{
  private static class TestMeta extends TestWatcher
  {
    private static final long ONE_SECOND = 1000L;
    private String applicationPath;
    private BandwidthManager underTest;
    private Context.OperatorContext context;
    private long bandwidthLimit = 10L;
    private ScheduledExecutorService mockschedular;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      mockschedular = new ScheduledExecutorService()
      {
        private Runnable command;

        @Override
        public void shutdown()
        {
        }

        @Override
        public List<Runnable> shutdownNow()
        {
          return null;
        }

        @Override
        public boolean isShutdown()
        {
          return false;
        }

        @Override
        public boolean isTerminated()
        {
          return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
          return false;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
          return null;
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
          return null;
        }

        @Override
        public Future<?> submit(Runnable task)
        {
          return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
        {
          return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
        {
          return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
        {
          return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
          return null;
        }

        @Override
        public void execute(Runnable command)
        {
          this.command.run();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
          this.command = command;
          return null;
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
          return null;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
          this.command = command;
          return null;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
          this.command = command;
          return null;
        }
      };
      underTest = new BandwidthManager(mockschedular);
      underTest.setBandwidth(bandwidthLimit);

      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_PATH, applicationPath);
      context = mockOperatorContext(1, attributes);
      underTest.setup(context);
    }

    @Override
    protected void finished(Description description)
    {
      underTest.teardown();
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testBandwidthForLargeBlocks() throws InterruptedException
  {
    String data = "Tuple: test data to be emitted.";
    long timeCounter = 0;
    testMeta.underTest.consumeBandwidth(data.length());
    while (!testMeta.underTest.canConsumeBandwidth()) {
      timeCounter += TestMeta.ONE_SECOND;
      testMeta.mockschedular.execute(null); // accumulate bandwidth
    }
    Assert.assertTrue(timeCounter > ((data.length() / testMeta.bandwidthLimit) * 1000));
  }

  @Test
  public void testBandwidthForSmallBlocks()
  {
    String data = "Tuple";
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(data.length());
    testMeta.mockschedular.execute(null);
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(data.length());
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(data.length());
    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth());
  }

  @Test
  public void testBandwidthForMultipleBlocks()
  {
    int[] tupleSizes = {5, 2, 5, 4, 10, 25, 2};
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(tupleSizes[0]);
    testMeta.mockschedular.execute(null);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(tupleSizes[1]);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(tupleSizes[2]);
    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth());
    testMeta.mockschedular.execute(null);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(tupleSizes[3]);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(tupleSizes[4]);
    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth());
    testMeta.mockschedular.execute(null);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(tupleSizes[5]);

    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth());
    testMeta.mockschedular.execute(null);
    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth());
    testMeta.mockschedular.execute(null);
    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth());
    testMeta.mockschedular.execute(null);
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
    testMeta.underTest.consumeBandwidth(tupleSizes[6]);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
  }

  @Test
  public void testUnsetBandwidth()
  {
    testMeta.underTest.setBandwidth(Integer.MAX_VALUE);
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth());
  }
}



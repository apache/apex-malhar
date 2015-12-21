/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.appdata.query;

import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.netlet.util.DTThrowable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

public class WindowBoundedServiceWindowDelayTest
{
  @Rule
  public WindowBoundedServiceWindowDelayTestWatcher testMeta = new WindowBoundedServiceWindowDelayTestWatcher();

  public static class WindowBoundedServiceWindowDelayTestWatcher extends TestWatcher
  {
    public ExecutorService executorService;

    @Override
    protected void starting(Description description)
    {
      executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    protected void finished(Description description)
    {
      executorService.shutdownNow();
    }
  }

  @Test
  public void onceAWindowTest()
  {
    Runnable runnable = new Runnable(){
      @Override
      public void run() {
        IncrementRunnable ir = new IncrementRunnable(0L);

        WindowBoundedServiceWindowDelay wbs =
        new WindowBoundedServiceWindowDelay(ir);

        wbs.setWindowsPerExecution(1L);

        Assert.assertEquals(0, ir.getCount());

        LOG.debug("Before setup");
        wbs.setup(null);
        LOG.debug("After setup");

        wbs.beginWindow(0);
        LOG.debug("After beginWindow(0)");
        TestUtils.sleep(100L);
        wbs.endWindow();
        LOG.debug("After endWindow(0)");
        TestUtils.sleep(100L);

        Assert.assertEquals(1, ir.getCount());

        wbs.beginWindow(1);
        LOG.debug("After beginWindow(1)");
        TestUtils.sleep(100L);
        wbs.endWindow();
        LOG.debug("After endWindow(1)");
        TestUtils.sleep(100L);

        LOG.debug("{}", ir.getCount());
        Assert.assertEquals(2, ir.getCount());

        wbs.teardown();
      }
    };

    testHelper(runnable);
  }

  @Test
  public void synchronizationTest()
  {
    Runnable runnable = new Runnable(){
      @Override
      public void run() {
        IncrementRunnable ir = new IncrementRunnable(300L);

        WindowBoundedServiceWindowDelay wbs =
        new WindowBoundedServiceWindowDelay(ir);

        wbs.setWindowsPerExecution(1L);

        Assert.assertEquals(0, ir.getCount());

        wbs.setup(null);

        wbs.beginWindow(0);
        wbs.endWindow();

        Assert.assertEquals(1, ir.getCount());

        wbs.beginWindow(1);
        wbs.endWindow();

        Assert.assertEquals(2, ir.getCount());

        wbs.teardown();
      }
    };

    testHelper(runnable);
  }

  @Test
  public void multiWindowTest()
  {
    Runnable runnable = new Runnable(){
      @Override
      public void run() {
        IncrementRunnable ir = new IncrementRunnable(0L);

        WindowBoundedServiceWindowDelay wbs =
        new WindowBoundedServiceWindowDelay(ir);

        wbs.setWindowsPerExecution(2L);

        Assert.assertEquals(0, ir.getCount());

        wbs.setup(null);

        for(long wid = 1L; wid < 5; wid++) {
          wbs.beginWindow(wid);
          TestUtils.sleep(100L);
          wbs.endWindow();
          TestUtils.sleep(100L);

          LOG.debug("wid {}", wid);
          Assert.assertEquals(wid / 2L, ir.getCount());
        }

        wbs.teardown();
      }
    };

    testHelper(runnable);
  }

  @Test
  public void throwExceptionTest()
  {
    Future<?> future = testMeta.executorService.submit(new ExceptionThrowerRunnable());

    testMeta.executorService.shutdown();

    try {
      Assert.assertTrue(testMeta.executorService.awaitTermination(10, TimeUnit.SECONDS));
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    boolean exceptionThrown = false;

    try {
      future.get();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } catch (ExecutionException ex) {
      exceptionThrown = ex.getCause() instanceof MockException;
    }

    Assert.assertTrue(exceptionThrown);
  }

  private void testHelper(Runnable runnable)
  {
    Future<?> future = testMeta.executorService.submit(runnable);

    testMeta.executorService.shutdown();

    try {
      Assert.assertTrue(testMeta.executorService.awaitTermination(10, TimeUnit.SECONDS));
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    try {
      future.get();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } catch (ExecutionException ex) {
      DTThrowable.rethrow(ex.getCause());
    }
  }

  public static class IncrementRunnable implements Runnable
  {
    private volatile int count = 0;
    private long delayMS = 0L;

    public IncrementRunnable(long delayMS)
    {
      this.delayMS = delayMS;
    }

    @Override
    public void run()
    {
      TestUtils.sleep(delayMS);
      LOG.debug("Incrementing");
      count++;
    }

    public int getCount()
    {
      return count;
    }
  }

  public static class ExceptionThrowerRunnable implements Runnable
  {
    @Override
    public void run()
    {
      throw new MockException();
    }
  }

  public static class MockException extends RuntimeException
  {
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(WindowBoundedServiceWindowDelayTest.class);
}

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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

import org.apache.apex.malhar.lib.appdata.query.QueryManagerAsynchronousTest.InterruptClear;

public class WindowBoundedServiceTest
{
  @Rule
  public TestWatcher testMeta = new InterruptClear();

  @Test
  public void simpleLoopTest() throws Exception
  {
    CounterRunnable counterRunnable = new CounterRunnable();

    WindowBoundedService wbs = new WindowBoundedService(1, counterRunnable);
    wbs.setup(null);
    Thread.sleep(500);
    Assert.assertEquals(0, counterRunnable.getCounter());
    wbs.beginWindow(0);
    Thread.sleep(500);
    wbs.endWindow();
    int currentCount = counterRunnable.getCounter();
    Thread.sleep(500);
    wbs.teardown();
    Assert.assertEquals(currentCount, counterRunnable.getCounter());
  }

  @Test
  public void runTest() throws Exception
  {
    CounterRunnable counterRunnable = new CounterRunnable();

    WindowBoundedService wbs = new WindowBoundedService(1, counterRunnable);
    wbs.setup(null);
    wbs.beginWindow(0);
    Thread.sleep(500);
    wbs.endWindow();
    wbs.teardown();
    Assert.assertTrue(counterRunnable.getCounter() > 0);
  }

  public static class CounterRunnable implements Runnable
  {
    private int counter = 0;

    public CounterRunnable()
    {
    }

    @Override
    public void run()
    {
      counter++;
    }

    public int getCounter()
    {
      return counter;
    }
  }

  public static class ExceptionRunnable implements Runnable
  {
    public ExceptionRunnable()
    {
    }

    @Override
    public void run()
    {
      throw new RuntimeException("Simulate Failure");
    }
  }
}

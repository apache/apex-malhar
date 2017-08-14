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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This class asynchronously executes a function so that the function is only called between calls
 * to {@link Operator#beginWindow} and {@link Operator#endWindow}.<br/><br/>
 * This service works by asynchronously calling its {@link #execute} method only after
 * {@link #beginWindow} and called and before {@link #endWindow} ends. Calls to {@link #beginWindow}
 * and {@link endWindow} will happen in the enclosing {@link Operator}'s main thread.
 * <br/><br/>
 * <b>Note:</b> This service cannot be used in operators which allow checkpointing within an
 * application window.
 *
 * @since 3.2.0
 */
public class WindowBoundedService implements Component<OperatorContext>
{
  public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 10;

  /**
   * The execute interval period in milliseconds.
   */
  private final long executeIntervalMillis;
  /**
   * The code to execute asynchronously.
   */
  private final Runnable runnable;
  protected transient ExecutorService executorThread;

  private final transient Semaphore mutex = new Semaphore(0);
  private volatile boolean terminated = false;

  public WindowBoundedService(Runnable runnable)
  {
    this.executeIntervalMillis = DEFAULT_FLUSH_INTERVAL_MILLIS;
    this.runnable = Preconditions.checkNotNull(runnable);
  }

  public WindowBoundedService(long executeIntervalMillis, Runnable runnable)
  {
    Preconditions.checkArgument(executeIntervalMillis > 0, "The executeIntervalMillis must be positive");
    this.executeIntervalMillis = executeIntervalMillis;
    this.runnable = Preconditions.checkNotNull(runnable);
  }

  @Override
  public void setup(OperatorContext context)
  {
    executorThread = Executors.newSingleThreadExecutor(new NameableThreadFactory("Query Executor Thread"));
    executorThread.submit(new AsynchExecutorThread());
  }

  public void beginWindow(long windowId)
  {
    mutex.release();
  }

  public void endWindow()
  {
    try {
      mutex.acquire();
    } catch (InterruptedException ex) {
      DTThrowable.wrapIfChecked(ex);
    }
  }

  @Override
  public void teardown()
  {
    LOG.info("Shutting down");
    terminated = true;
    mutex.release();

    executorThread.shutdown();

    try {
      executorThread.awaitTermination(10000L + executeIntervalMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      //Do nothing
    }
  }

  public class AsynchExecutorThread implements Callable<Void>
  {
    private long lastExecuteTime = 0;

    public AsynchExecutorThread()
    {
    }

    @Deprecated
    public AsynchExecutorThread(Thread mainThread)
    {
      //Do nothing
    }

    @Override
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch"})
    public Void call() throws Exception
    {
      try {
        loop();
      } catch (Exception e) {
        LOG.error("Exception thrown while processing:", e);
        mutex.release();
      }

      return null;
    }

    @SuppressWarnings("SleepWhileInLoop")
    private void loop() throws Exception
    {
      while (true) {
        long currentTime = System.currentTimeMillis();
        long diff = currentTime - lastExecuteTime;

        if (diff > executeIntervalMillis) {
          lastExecuteTime = currentTime;
          mutex.acquireUninterruptibly();

          if (terminated) {
            LOG.info("Terminated");
            return;
          }

          runnable.run();
          mutex.release();
        } else {

          if (terminated) {
            LOG.info("Terminated");
            return;
          }

          Thread.sleep(executeIntervalMillis - diff);
        }
      }
    }
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(WindowBoundedService.class);
}

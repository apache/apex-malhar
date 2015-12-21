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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This class asynchronously executes a function so that the function is only called between calls
 * to {@link com.datatorrent.api.Operator#beginWindow(long)} and {@link com.datatorrent.api.Operator#endWindow} once
 * every N windows.<br/><br/>
 * Calls to {@link #beginWindow}
 * and {@link #endWindow} will happen in the enclosing {@link com.datatorrent.api.Operator}'s main thread.
 * <br/><br/>
 * <b>Note:</b> This service cannot be used in operators which allow checkpointing within an
 * application window.
 */
public class WindowBoundedServiceWindowDelay implements Component<Context.OperatorContext>
{
  /**
   * The default number of milliseconds to wait while polling to see if the asynchronous function is done.
   */
  public static final long SLEEP_MILLIS = 10L;

  /**
   * The number of milliseconds to wait while polling to see if the asynchronous function is done.
   */
  @Min(1)
  private long sleepMillis = SLEEP_MILLIS;
  /**
   * The number of windows that must pass before the provided function is executed.
   */
  @Min(1)
  private long windowsPerExecution = 1;
  /**
   * The function to execute every windowsPerExecution (N) windows.
   */
  @NotNull
  private final Runnable runnable;

  /**
   * The number of windows that have passed.
   */
  private long windowCount = 1L;

  /**
   * This {@link Semaphore} is used to ensure execution does not proceed to the next
   * window until the provided {@link Runnable} has completed executing.
   */
  private transient Semaphore waiter = new Semaphore(0);
  /**
   * The {@link ExecutorService} used to 
   */
  private transient ExecutorService executorService;
  /**
   * The {@link Future} returned when running the provided {@link Runnable}
   */
  private transient Future<Void> future;

  /**
   * Creates a {@link WindowBoundedServiceWindowDelay} when executes the given {@link Runnable}
   * periodically.
   * @param runnable The {@link Runnable} to execute periodically. 
   */
  public WindowBoundedServiceWindowDelay(@NotNull Runnable runnable)
  {
    this.runnable = Preconditions.checkNotNull(runnable);
  }

  @Override
  public void setup(Context.OperatorContext cntxt)
  {
    executorService = Executors.newSingleThreadExecutor(new NameableThreadFactory("Window Bounded Thread"));
  }

  public void beginWindow(long windowId)
  {
    if (windowCount == windowsPerExecution) {
      //Time to execute the service.
      windowCount = 0;
      future = executorService.submit(new WindowSyncRunnableWrapper(runnable));
    } else {
      //Rlease the lock so it can be acquired without delay.
      waiter.release();
    }
  }

  public void endWindow()
  {
    try {
      //Wait here until runnable finishes executing.
      waiter.acquire();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    if (future != null) {

      while (!future.isDone()) {
        //Handle wait condition where the runnable signaled it's done
        //but the future was not marked as done.
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }

      try {
        //Make sure any exceptions in the child thread are rethrown.
        future.get();
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      } catch (ExecutionException ex) {
        DTThrowable.rethrow(ex.getCause());
      }

      future = null;
    }

    windowCount++;
  }

  @Override
  public void teardown()
  {
    executorService.shutdown();
  }

  /**
   * Gets the windowsPerExecution.
   * @return The windowsPerExecution.
   */
  public long getWindowsPerExecution()
  {
    return windowsPerExecution;
  }

  /**
   * Sets the windowsPerExecution. The windowsPerExecution is the number of windows
   * that must elapse until the specified {@link Runnable} is executed. If windowsPerExecution
   * is 1, then the specified {@link Runnable} is executed every window. If windowsPerExecution
   * is 2, then the specified {@link Runnable} is executed every other window.
   * @param windowsPerExecution The windowsPerExecution.
   */
  public void setWindowsPerExecution(long windowsPerExecution)
  {
    Preconditions.checkArgument(windowsPerExecution >= 1);
    this.windowsPerExecution = windowsPerExecution;
  }

  /**
   * Gets the sleepMillis.
   * @return The sleepMillis.
   */
  public long getSleepMillis()
  {
    return sleepMillis;
  }

  /**
   * Sets the sleepMillis. sleepMillis is the number of milliseconds
   * that the main operator thread sleeps when it polls the {@link Runnable} task
   * to see if it's done.
   * @param sleepMillis sleepMillis is the number of milliseconds
   * that the main operator thread sleeps when it polls the {@link Runnable} task
   * to see if it's done.
   */
  public void setSleepMillis(long sleepMillis)
  {
    Preconditions.checkArgument(sleepMillis <= 0L);
    this.sleepMillis = sleepMillis;
  }

  /**
   * This class is a wrapped around the {@link Runnable} that is
   * executed. The wrapper ensures that the main operator thread is
   * signaled when it's appropriate for the window to continue.
   */
  protected class WindowSyncRunnableWrapper implements Callable<Void>
  {
    /**
     * The {@link Runnable} to wrap.
     */
    @NotNull
    private final Runnable runnable;

    /**
     * This creates a {@link WindowSyncRunnableWrapper} which wraps the
     * given {@link Runnable}
     * @param runnable The {@link Runnable} to wrap.
     */
    public WindowSyncRunnableWrapper(@NotNull Runnable runnable)
    {
      this.runnable = Preconditions.checkNotNull(runnable);
    }

    @Override
    public Void call() throws Exception
    {
      try {
        runnable.run();
      } finally {
        waiter.release();
      }

      return null;
    }
  }
}

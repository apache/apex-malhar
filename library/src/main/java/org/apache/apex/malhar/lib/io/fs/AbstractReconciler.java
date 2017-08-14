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
package org.apache.apex.malhar.lib.io.fs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This base operator queues input tuples for each window and asynchronously processes them after the window is committed.
 *
 * The operator holds all the tuple info in memory until the committed window and then calls the processCommittedData method
 * to give an opportunity to process tuple info from each committed window.
 *
 * This operator can be implemented to asynchronously read and process data that is being written by current application.
 *
 * Use case examples: write to relational database, write to an external queue etc without blocking the dag i/o.
 *
 * @param <INPUT>      input type
 * @param <QUEUETUPLE> tuple enqueued each window to be processed after window is committed
 * @since 2.0.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractReconciler<INPUT, QUEUETUPLE> extends BaseOperator implements CheckpointNotificationListener, IdleTimeHandler
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractReconciler.class);
  public transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT input)
    {
      processTuple(input);
    }

  };
  protected transient ExecutorService executorService;
  protected long currentWindowId;
  protected transient int spinningTime = 10;
  // this stores the mapping from the window to the list of enqueued tuples
  private Map<Long, List<QUEUETUPLE>> currentWindowTuples = Maps.newConcurrentMap();
  private Queue<Long> currentWindows = Queues.newLinkedBlockingQueue();
  protected Queue<QUEUETUPLE> committedTuples = Queues.newLinkedBlockingQueue();
  protected transient Queue<QUEUETUPLE> doneTuples = Queues.newLinkedBlockingQueue();
  private transient Queue<QUEUETUPLE> waitingTuples = Queues.newLinkedBlockingQueue();
  private transient volatile boolean execute;
  private transient AtomicReference<Throwable> cause;

  @AutoMetric
  private long queueLength;


  @Override
  public void setup(Context.OperatorContext context)
  {
    if (context != null) {
      spinningTime = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    }
    execute = true;
    cause = new AtomicReference<Throwable>();
    waitingTuples.addAll(committedTuples);
    executorService = Executors.newSingleThreadExecutor(new NameableThreadFactory("Reconciler-Helper"));
    executorService.submit(processEnqueuedData());
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    currentWindowTuples.put(currentWindowId, new ArrayList<QUEUETUPLE>());
    currentWindows.add(windowId);
  }

  @Override
  public void endWindow()
  {
    while (doneTuples.peek() != null) {
      committedTuples.remove(doneTuples.poll());
    }
  }

  @Override
  public void handleIdleTime()
  {
    if (execute) {
      try {
        Thread.sleep(spinningTime);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    } else {
      logger.error("Exception: ", cause);
      DTThrowable.rethrow(cause.get());
    }

  }

  @Override
  public void beforeCheckpoint(long l)
  {
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long l)
  {
    logger.debug(" current committed window {}", l);
    if (currentWindows.isEmpty()) {
      return;
    }
    long processedWindowId = currentWindows.peek();
    while (processedWindowId <= l) {
      List<QUEUETUPLE> outputDataList = currentWindowTuples.get(processedWindowId);
      if (outputDataList != null && !outputDataList.isEmpty()) {
        committedTuples.addAll(outputDataList);
        waitingTuples.addAll(outputDataList);
      }
      currentWindows.remove();
      currentWindowTuples.remove(processedWindowId);
      if (currentWindows.isEmpty()) {
        return;
      }
      processedWindowId = currentWindows.peek();
    }
  }

  @Override
  public void teardown()
  {
    execute = false;
    executorService.shutdownNow();
  }

  private Runnable processEnqueuedData()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        try {
          while (execute) {
            while (waitingTuples.isEmpty()) {
              Thread.sleep(spinningTime);
            }
            QUEUETUPLE output = waitingTuples.remove();
            processCommittedData(output);
            --queueLength;
            doneTuples.add(output);
          }
        } catch (Throwable e) {
          cause.set(e);
          execute = false;
        }
      }
    };
  }

  /**
   * The implementation class should call this method to enqueue output once input is converted to queue input.
   *
   * The queueTuple is processed once the window in which queueTuple is enqueued is committed.
   *
   * @param queueTuple
   */
  protected void enqueueForProcessing(QUEUETUPLE queueTuple)
  {
    currentWindowTuples.get(currentWindowId).add(queueTuple);
    ++queueLength;
  }

  /**
   * Process input tuple
   *
   * @param input
   */
  protected abstract void processTuple(INPUT input);

  /**
   * This method is called once the window in which queueTuple was created is committed.
   * Implement this method to define the functionality to synchronize data.
   *
   * @param queueInput
   */
  protected abstract void processCommittedData(QUEUETUPLE queueInput);
}

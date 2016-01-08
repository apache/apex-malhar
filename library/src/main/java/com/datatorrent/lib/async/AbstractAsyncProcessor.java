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
package com.datatorrent.lib.async;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.common.util.BaseOperator;


/**
 * This base operator can be used to enqueue tuples for asynchronous and parallel processing by separate worker threads.
 *
 * User can configure number of threads required for async processing. By default
 * threads will be created on need basis with no upper limit. These worker threads, spawned by operator thread,
 * can also be configured to processing timeout.
 *
 * <b>Workflow is as follows:</b>
 * <ol>
 *   <li>Implementing class can enqueue the tuples of type QUEUETUPLE using <i>enqueueTupleForProcessing</i>
 *       method.</li>
 *   <li>When a tuple gets its turn for execution, <i>processTupleAsync</i> gets called
 *       by worker thread. This is asynchronous call</li>
 *   <li>Based on maintainTupleOrder parameter, the results are consolidated at the end of every window and respective
 *       call to <i>handleProcessedTuple</i> will be made. This callback is in operator thread.</li>
 * </ol>
 *
 *
 * <b>Use case examples:</b>
 * <ul>
 *   <li>Parallel reads from external datastore/database system.</li>
 *   <li>Operations which are long running tasks per tuple but DAG i/o should not be blocked.</li>
 * </ul>
 *
 * @param <QUEUETUPLE> input type of tuple
 * @param <RESULTTUPLE> Result of async processing of tuple.
 */
public abstract class AbstractAsyncProcessor<QUEUETUPLE, RESULTTUPLE> extends BaseOperator implements IdleTimeHandler
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractAsyncProcessor.class);

  /**
   * This is an executor service which holds thread pool and enqueues task for processing.
   */
  private transient ExecutorService executor;

  /**
   * Number of worker threads for processing
   * 0   - Threads will be created as per need.
   * >=1 - Fixed number of thread pool will be created.
   */
  @Min(0)
  private int numProcessors = 0;

  /**
   * Tells whether the <i>handleProcessedTuple</i> should be called for tuples in the same order in which
   * they get enqueued using <i>enqueueTupleForProcessing</i>.
   */
  private boolean maintainTupleOrder = false;

  /**
   * Timeout in milliseconds after which processing of tuple may be terminated.
   */
  private long processTimeoutMs = 0;

  /**
   * The queue holds following types of tuples:
   * <ul>
   *   <li>Yet to be processed</li>
   *   <li>In Process</li>
   *   <li>Completed processing but yet to be notified for process completion</li>
   * </ul>
   */
  private Queue<WorkItem> waitingTuples = Lists.newLinkedList();

  /**
   * Setup method of this operator will initialize various types of executors possible.
   *
   * @param context OperatorContext
   */
  @Override public void setup(Context.OperatorContext context)
  {
    if (numProcessors == 0) {
      this.executor = Executors.newCachedThreadPool();
    } else {
      this.executor = Executors.newFixedThreadPool(numProcessors);
    }

    replayTuplesIfRequired();
  }

  /**
   * This method is called from setup of the operator. If <i>waitingTuples</i> contains anything, the method enqueues
   * all those tuples for processing to worker threads.
   *
   * Dependening on condition the method will behave as follows:
   * <ul>
   *   <li>During fresh start of operator, this method will not have any effect as there are no tuples in
   *       <i>waitingTuples</i>.</li>
   *   <li>During restore of operator to previous checkpointed state, if there are any tuples already present
   *       in <i>waitingTuples</i> that means they're yet to be notified, hence enqueued again.</li>
   * </ul>
   */
  private void replayTuplesIfRequired()
  {
    // Enqueue everything that is still left.
    // Its left because its either not completed processing OR its not emitted because of ordering restrictions.
    for (WorkItem item : waitingTuples) {
      item.taskHandle = executor.submit(new Processor(item));
      item.processState = State.SUBMITTED;
      item.processStartTime = 0;
      item.outTuple = null;
    }
  }

  /**
   * Teardown of operator which stops executor service.
   */
  @Override public void teardown()
  {
    this.executor.shutdownNow();
  }

  /**
   * Enqueues tuple of type QUEUETUPLE for processing to worker threads.
   * This method should be called only from operator thread.
   *
   * @param QUEUETUPLE Input tuple of type QUEUETUPLE
   */
  protected void enqueueTupleForProcessing(QUEUETUPLE QUEUETUPLE)
  {
    WorkItem item = new WorkItem(QUEUETUPLE);
    waitingTuples.add(item);
    item.taskHandle = executor.submit(new Processor(item));
    item.processState = State.SUBMITTED;
  }

  /**
   * Abstract method which gets called from AsyncProcessor when results for certain input tuple
   * are ready to be notified.
   * The method is called from operator thread hence this is synchronous method of this operator.
   *
   * <b>
   *   The implementer of this method should keep this method to minimal overhead because this being part of operator
   *   thread, the long running tasks may block DAG i/o.
   * </b>
   *
   * @param inpTuple     Input tuple which was processed of type QUEUETUPLE
   * @param resultTuple  Result tuple of type RESULTTUPLE
   * @param processState Processing State of input tuple which will be one of SUCCESS, FAILED or TIMEOUT.
   */
  protected abstract void handleProcessedTuple(QUEUETUPLE inpTuple, RESULTTUPLE resultTuple, State processState);

  /**
   * Abstract method which gets called from Worker threads for processing given input tuple of type QUEUETUPLE.
   * The method will be called in Async mode.
   * The implementer of this method can chose to have long running tasks here.
   *
   * @param tuple Input tuple to be processed of type QUEUETUPLE
   * @return Result of processing of input tuple of type RESULTTUPLE.
   */
  protected abstract RESULTTUPLE processTupleAsync(QUEUETUPLE tuple);

  /**
   * This method will be called from endWindow/handleIdleTimeout where any ready to notify tuple will be identified and
   * notification will be done.
   */
  private void checkFinishedTuples()
  {
    Iterator<WorkItem> listIterator = waitingTuples.iterator();
    while (listIterator.hasNext()) {
      WorkItem item = listIterator.next();
      switch (item.processState) {
        case SUCCESS:
          // Tuple processing is completed successfully. As this is in order its safe to emit.
          handleProcessedTuple(item.inpTuple, item.outTuple, item.processState);
          listIterator.remove();
          break;
        case FAILED:
          // Tuple processing is completed in a failed state. As this is in order its safe to emit.
          handleProcessedTuple(item.inpTuple, item.outTuple, item.processState);
          listIterator.remove();
          break;
        case INPROCESS:
          // Tuple is in process.
          if ((processTimeoutMs != 0) && ((System.currentTimeMillis() - item.processStartTime)
              > processTimeoutMs)) {
            // Tuple has be in process for quite some time. Stop it.
            item.taskHandle.cancel(true);
            item.processState = State.TIMEOUT;
            handleProcessedTuple(item.inpTuple, item.outTuple, item.processState);
            listIterator.remove();
          } else {
            // Tuple is in process and has not exceed timeout OR there is no timeout.
            if (maintainTupleOrder) {
              // We have to stop here as this particular tuple has not finished processing.
              return;
            }
          }
          break;
        case SUBMITTED:
          // Processing not started. Just submitted.
          if (maintainTupleOrder) {
            // We have to stop here as this particular tuple has not started processing.
            return;
          }
          break;
        default:
          // Still in init state. That's not possible. Throw.
          throw new RuntimeException("Unexpected condition met. WorkItem cannot be in INIT state.");
      }
    }
  }

  /**
   * If some async operations are finished, they're check and notified here.
   */
  @Override public void handleIdleTime()
  {
    checkFinishedTuples();
  }

  /**
   * If some async operations are finished, they're check and notified here.
   */
  @Override public void endWindow()
  {
    checkFinishedTuples();
  }

  public int getNumProcessors()
  {
    return numProcessors;
  }

  public void setNumProcessors(int numProcessors)
  {
    this.numProcessors = numProcessors;
  }

  public boolean isMaintainTupleOrder()
  {
    return maintainTupleOrder;
  }

  public void setMaintainTupleOrder(boolean maintainTupleOrder)
  {
    this.maintainTupleOrder = maintainTupleOrder;
  }

  public long getProcessTimeoutMs()
  {
    return processTimeoutMs;
  }

  public void setProcessTimeoutMs(long processTimeoutMs)
  {
    this.processTimeoutMs = processTimeoutMs;
  }

  /**
   * This is a main Worker thread which will call processTupleAsync method for async processing of tuple.
   */
  private class Processor implements Runnable
  {
    private WorkItem tupleToProcess;

    public Processor(WorkItem tupleToProcess)
    {
      this.tupleToProcess = tupleToProcess;
    }

    @Override public void run()
    {
      this.tupleToProcess.processStartTime = System.currentTimeMillis();
      this.tupleToProcess.processState = State.INPROCESS;
      try {
        this.tupleToProcess.outTuple = AbstractAsyncProcessor.this.processTupleAsync(this.tupleToProcess.inpTuple);
        this.tupleToProcess.processState = State.SUCCESS;
      } catch (Throwable e) {
        logger.warn("Failed to execute the operation: {}", e.getMessage());
        this.tupleToProcess.processState = State.FAILED;
      }
    }
  }

  public enum State
  {
    INIT, SUBMITTED, INPROCESS, SUCCESS, FAILED, TIMEOUT
  }

  /**
   * WorkItem class will hold the various information of processing tuples.
   */
  private class WorkItem
  {
    QUEUETUPLE inpTuple;
    transient RESULTTUPLE outTuple;
    transient Future taskHandle;
    transient State processState;
    transient long processStartTime;

    protected WorkItem()
    {
      // For kryo
    }

    public WorkItem(QUEUETUPLE inpTuple)
    {
      this.inpTuple = inpTuple;
      this.outTuple = null;
      this.processStartTime = 0;
      this.processState = State.INIT;
    }
  }

}

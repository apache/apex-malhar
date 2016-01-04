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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * This base operator having a single input and tuple coming from input port are enqueued for processing asynchronously
 * by Thread Executors.
 *
 * User can configure number of of threads that are required for processing. Defaulting to single thread which is
 * different than operator thread.
 * The user will also configure the timeout for the operations to happen.
 *
 * <b>Workflow is as follows:</b>
 * <ol>
 *   <li>Tuple of type INPUT is received on input port.</li>
 *   <li>Tuple gets enqueued into Executors by calling <i>enqueueTupleForProcessing</i>.
 *       Once can override <i>processTuple</i> method in which case, its users responsibility to enqueue tuple for
 *       processing by calling <i>enqueueTupleForProcessing</i>.</li>
 *   <li>When tuple gets its turn of execution, <i>processTupleAsync</i> gets called from thread other than
 *       operator thread.</li>
 *   <li>Based on maintainTupleOrder parameter, the results are consolidated at end of every window and respective call
 *       to <i>handleProcessedTuple</i> will be made. This call will happen in operator thread.</li>
 * </ol>
 *
 * This operator can be implemented to do async and parallel operations.
 *
 * Use case examples:
 * <ul>
 *   <li>Parallel reads from external datastore/database system.</li>
 *   <li>Doing any operations which are long running tasks per tuple but DAG io should not be blocked.</li>
 * </ul>
 *
 * @param <INPUT> input type of tuple
 * @param <RESULT> Result of async processing of tuple.
 * @since 3.3.0
 */
public abstract class AbstractAsyncProcessor<INPUT, RESULT> extends BaseOperator implements IdleTimeHandler
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractAsyncProcessor.class);

  private transient ExecutorService executor;
  private int numProcessors = 1;
  private boolean maintainTupleOrder = false;
  private long processTimeoutMs = 0;

  /**
   * This Queue will hold all the tuples that are in process, has completed processing but yet to be notified for
   * processing completion.
   */
  private Queue<ProcessTuple> waitingTuples = Lists.newLinkedList();

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override public void process(INPUT input)
    {
      processTuple(input);
    }

    @Override public void setup(Context.PortContext context)
    {
      setupInputPort(context);
    }
  };

  /**
   * This method is exposed for implementing class so as to take care of input port based initialization.
   * For eg. Retrieve TUPLE_CLASS attribute from input port.
   *
   * @param context PortContext of input port
   */
  protected void setupInputPort(Context.PortContext context)
  {
    // Do nothing. Concrete class may choose to do override this. For eg. reading TUPLE_CLASS attribute.
  }

  /**
   * Setup method of this operator will initialize the various types of executors possible.
   *
   * @param context OperatorContext
   */
  @Override public void setup(Context.OperatorContext context)
  {
    if (numProcessors == 1) {
      this.executor = Executors.newSingleThreadExecutor();
    } else if (numProcessors == 0) {
      this.executor = Executors.newCachedThreadPool();
    } else if (numProcessors > 0) {
      this.executor = Executors.newFixedThreadPool(numProcessors);
    } else {
      throw new RuntimeException("Number of processors cannot be -ve. Value set is: " + numProcessors);
    }

    replayTuplesIfRequired();
  }

  /**
   * This method will be called from setup of this operator. The method will enqueue all the tuples for prcessing to
   * Executors which are pre-existing in processing queue i.e. <i>waitingTuples</i> variable.
   * The <i>waitingTuple</i> is expected to contain anything only if the operators gets restored back to previous
   * checkpointed state.
   */
  private void replayTuplesIfRequired()
  {
    // Enqueue everything that is still left.
    // Its left because its either not completed processing OR its not emitted because of ordering restrictions.
    for (ProcessTuple processTuple : waitingTuples) {
      processTuple.taskHandle = executor.submit(new Processor(processTuple));
      processTuple.processState = State.SUBMITTED;
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
   * This method is called from process of input port.
   * The default behaviour of this method is to enqueue the input tuple for processing to Executors.
   *
   * One can chose to override this method to have additional action and then enqueue the tuple for processing.
   *
   * @param input Input tuple of type INPUT.
   */
  protected void processTuple(INPUT input)
  {
    enqueueTupleForProcessing(input);
  }

  /**
   * Enqueues received INPUT tuple for processing to Executors.
   * This method should be called only from operator thread.
   *
   * @param input Input tuple of type INPUT
   */
  protected void enqueueTupleForProcessing(INPUT input)
  {
    ProcessTuple processTuple = new ProcessTuple(input);
    waitingTuples.add(processTuple);
    processTuple.taskHandle = executor.submit(new Processor(processTuple));
    processTuple.processState = State.SUBMITTED;
  }

  /**
   * Abstract method which will get called from this AsyncProcessor when the results for certain input tuple
   * are ready to be notified.
   * The method will be called from operator thread hence this is a sync method of this operator.
   *
   * The implementer of this method should keep this method to minimal overhead.
   *
   * @param inpTuple     Input tuple which was processed of type INPUT
   * @param resultTuple  Result tuple of type RESULT
   * @param processState Processing State of input tuple which will be one of SUCCESS, FAILED or TIMEOUT.
   */
  protected abstract void handleProcessedTuple(INPUT inpTuple, RESULT resultTuple, State processState);

  /**
   * Abstract method which will get called from Executor threads for processing given input tuple of type INPUT.
   * The method will be called in Async mode.
   * The implementer of this method can chose to have long running tasks here.
   *
   * @param tuple Input tuple to be processed of type INPUT
   * @return Result of processing of input tuple of type RESULT.
   */
  protected abstract RESULT processTupleAsync(INPUT tuple);

  /**
   * This method will be called from endWindow where any ready to notify tuple will be identified and
   * notification will be done.
   */
  private void emitFinishedTuple()
  {
    Iterator<ProcessTuple> listIterator = waitingTuples.iterator();
    while (listIterator.hasNext()) {
      ProcessTuple processTuple = listIterator.next();
      switch (processTuple.processState) {
        case SUCCESS:
          // Tuple processing is completed successfully. As this is in order its safe to emit.
          handleProcessedTuple(processTuple.inpTuple, processTuple.outTuple, processTuple.processState);
          listIterator.remove();
          break;
        case FAILED:
          // Tuple processing is completed in a failed state. As this is in order its safe to emit.
          handleProcessedTuple(processTuple.inpTuple, processTuple.outTuple, processTuple.processState);
          listIterator.remove();
          break;
        case INPROCESS:
          // Tuple is in process.
          if ((processTimeoutMs != 0) && ((System.currentTimeMillis() - processTuple.processStartTime)
              > processTimeoutMs)) {
            // Tuple has be in process for quite some time. Stop it.
            processTuple.taskHandle.cancel(true);
            processTuple.processState = State.TIMEOUT;
            handleProcessedTuple(processTuple.inpTuple, processTuple.outTuple, processTuple.processState);
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
          throw new RuntimeException("Unexpected condition met. ProcessTuple cannot be in INIT state.");
      }
    }
  }

  /**
   * If some async operations are finished, they're check and notified here.
   */
  @Override public void handleIdleTime()
  {
    emitFinishedTuple();
  }

  /**
   * If some async operations are finished, they're check and notified here.
   */
  @Override public void endWindow()
  {
    emitFinishedTuple();
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
   * This is a mail Executor thread which will call processTupleAsync method for async processing of tuple.
   */
  private class Processor implements Runnable
  {
    private ProcessTuple tupleToProcess;

    public Processor(ProcessTuple tupleToProcess)
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
   * ProcessTuple class will hold the various information of processing tuples.
   */
  public class ProcessTuple
  {
    INPUT inpTuple;
    transient RESULT outTuple;
    transient Future taskHandle;
    transient State processState;
    transient long processStartTime;

    protected ProcessTuple()
    {
      // For kryo
    }

    public ProcessTuple(INPUT inpTuple)
    {
      this.inpTuple = inpTuple;
      this.outTuple = null;
      this.processStartTime = 0;
      this.processState = State.INIT;
    }
  }

}

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
package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonEngine;
import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonInterpreterConfig;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;
import org.apache.apex.malhar.python.base.requestresponse.PythonCommandType;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkNotNull;

/***
 * <p>Implements the {@link ApexPythonEngine} interface by using the JEP ( Java Embedded Python ) engine. It is an
 *  in-memory interpreter and has the following characteristics:
 *  <ol>
 *    <li>The python engine allows for 4 major API patterns
 *    <ul>
 *      <li>Execute a method call by accepting parameters to pass to the interpreter</li>
 *      <li>Execute a python script as given in a file path</li>
 *      <li>Evaluate an expression and allows for passing of variables between the java code and the python
 *      in-memory interpreter bridge</li>
 *      <li>A handy method wherein a series of instructions can be passed in one single java call ( executed as a
 *       sequence of python eval instructions under the hood ) </li>
 *    </ul>
 *    <li>Automatic garbage collection of the variables that are passed from java code to the in memory python
 *     interpreter</li>
 *    <li>Support for all major python libraries. Tensorflow, Keras, Scikit, XGBoost</li>
 *    <li>The python engine uses the concept of a worker thread that is responsible for executing any of the 4
 *     patterns mentioned above. The worker thread is implemented by {@link InterpreterThread}</li>
 *    <li>The implementation allows for SLA based execution model. i.e. the caller can stipulate that if the call is not
 *     complete within a time out, the engine code returns back null. See {@link InterpreterWrapper}</li>
 *    <li>Supports the concept of stragglers i.e. the processing of a request can complete eventually and the
 *     result available from a queue called as the delayed response queue</li>
 *    <li>Supports the concept of executing a call on all of the worker threads if required. This is to ensure the
 *     following use cases:
 *      <ul>
 *        <li>Since this is an interpreter, the users can make use of an earlier calls variable definition if
 *         need be. In such cases, the caller will have the need for a sticky thread i.e. all such calls need to
 *          end up on the same thread.</li>
 *        <li>Another reason is to implement the concept of Dynamic partitioning. Since interpreter accumulates
 *         state due to commands run on it, if a new partition is introduced at runtime, this can failures for all
 *          subsequent commands as they might depend on variables created in previous windows</li>
 *      </ul>
 *    </li>
 *  </li>
 *  </ol>
 * </p>
 *
 * <p> Note that the engine implementation can be used independent of an Operator i.e. as a utility stack if need be.
 *  Some of the API signatures need a window ID and request ID but they do not necessarily mean that the API
 *   signatures are bound to an operator lifecycle. These parameters are used for efficient thread usage only and
 *    the API only needs a monotonically increasing number in true sense.
 * </p>
 *
 * <p>
 *   JEP needs to be installed on all of the YARN nodes prior to the use of the JEP engine until docker support is
 *    available for Apex. Virtual environments are not supported yet. If multiple versions of python are present
 *     on the YARN nodes, ensure  the JVM option java.library.path is pointing to the right version of JEP which in
 *      turn will ensure the right version of python to be used at runtime.
 * </p>
 *
 * <p>
 *   JEPPythonEngine works on the concept of a worker pool. The engine maintains a configurable number of workers and
 *    each worker has a dedicated request and response queue. While this class is responsible for choosing the
 *    right worker from the pool of workers for a given request , the {@link InterpreterWrapper} class is responsible
 *     for maintaining the time bound SLAs.
 * </p>
 *
 */
public class JepPythonEngine implements ApexPythonEngine
{
  private static final Logger LOG = LoggerFactory.getLogger(JepPythonEngine.class);

  /* Size of the worker pool */
  private int numWorkerThreads = 3;

  /* A name that can be used while logging messages and also used to set thread names */
  private String threadGroupName;

  private static final String JEP_LIBRARY_NAME = "jep";

  private transient List<PythonRequestResponse> commandHistory = new ArrayList<>();

  /* Spin policy for the disruptor queue implementation */
  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  // Represents the number of responses that can be held in the queue
  private int bufferCapacity = 64;

  /* Time used to sleep in the beginning of the interpreter threads run i.e. start  while initializing the interpreter.
  Note that booting of the memory interpreter can be a really heavy process depending on the libraries that
   are being loaded and hence this variable */
  private long sleepTimeAfterInterpreterStart = 3000; // 3 secs

  /**
   * Represents the queue into which all the stragglers are drained into
   */
  private transient BlockingQueue<PythonRequestResponse> delayedResponseQueue =
      new DisruptorBlockingQueue<>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private List<InterpreterWrapper> workers = new ArrayList<>();

  private Map<PythonInterpreterConfig, Object> preInitConfigs;

  private long numStarvedReturns = 0;

  /***
   * Created the JEP Python engine instance but does not start the interpreters yet
   * @param threadGroupName A name that represents all the workers in this thread
   * @param numWorkerThreads Number of workers in the work pool
   */
  public JepPythonEngine(String threadGroupName,int numWorkerThreads)
  {
    this.numWorkerThreads = numWorkerThreads;
    this.threadGroupName = threadGroupName;
  }

  private void initWorkers() throws ApexPythonInterpreterException
  {
    LOG.info("Attempting to load the JEP dynamic library");
    System.loadLibrary(JEP_LIBRARY_NAME);
    LOG.info("Successfully loaded the JEP dynamic library in memory");
    SpinPolicy spinPolicyForReqQueue = SpinPolicy.WAITING;
    if (preInitConfigs.containsKey(PythonInterpreterConfig.REQUEST_QUEUE_WAIT_SPIN_POLICY)) {
      spinPolicyForReqQueue = (SpinPolicy)preInitConfigs.get(PythonInterpreterConfig.REQUEST_QUEUE_WAIT_SPIN_POLICY);
    }
    for ( int i = 0; i < numWorkerThreads; i++) {
      InterpreterWrapper aWorker = new InterpreterWrapper(threadGroupName + "-" + i,delayedResponseQueue,
          spinPolicyForReqQueue);
      aWorker.preInitInterpreter(preInitConfigs);
      aWorker.startInterpreter();
      workers.add(aWorker);
    }
  }

  /***
   * Used to select the right worker from the work pool. The goal is to round robin the workers as far as possible.
   *  Factors like busy workers can mean that the next available worker is chosen
   * @param requestId Used to round robin the requests. Need not necessarily mean only an operator can use this engine.
   * @return A worker from the worker pool. Null if all workers are busy.
   */
  protected InterpreterWrapper selectWorkerForCurrentCall(long requestId)
  {
    int slotToLookFor = Ints.saturatedCast(requestId) % numWorkerThreads;
    LOG.debug("Slot that is being looked for in the worker pool " + slotToLookFor);
    boolean isWorkerFound = false;
    int numWorkersScannedForAvailability = 0;
    InterpreterWrapper aWorker = null;
    while ( (!isWorkerFound) && (numWorkersScannedForAvailability < numWorkerThreads)) {
      aWorker = workers.get(slotToLookFor);
      numWorkersScannedForAvailability  = numWorkersScannedForAvailability + 1;
      if (!aWorker.isCurrentlyBusy()) {
        isWorkerFound = true;
        LOG.debug("Found worker with index as  " + slotToLookFor);
        break;
      } else {
        LOG.debug("Thread ID is currently busy " + aWorker.getInterpreterId());
        slotToLookFor = slotToLookFor + 1;
        if ( slotToLookFor == numWorkerThreads) {
          slotToLookFor = 0;
        }
      }
    }
    if (isWorkerFound) {
      return aWorker;
    } else {
      numStarvedReturns += 1;
      return null;
    }
  }

  /***
   * See {@link ApexPythonEngine#preInitInterpreter(Map)} for more details
   * @param preInitConfigs The configuration that is going to be used by the interpreter.See constants
   *                       defined in {@link PythonInterpreterConfig} for a list of keys available
   * @throws ApexPythonInterpreterException if an issue while executing the pre interpreter logic
   */
  @Override
  public void preInitInterpreter(Map<PythonInterpreterConfig, Object> preInitConfigs)
    throws ApexPythonInterpreterException
  {
    this.preInitConfigs = preInitConfigs;
  }

  /***
   * Starts all of the worker threads. Also sleeps for a few moments to ensure "fat" frameworks like Tensorflow can
   *  be allowed to boot completely.
   * @throws ApexPythonInterpreterException
   */
  @Override
  public void startInterpreter() throws ApexPythonInterpreterException
  {
    initWorkers();
    try {
      if (sleepTimeAfterInterpreterStart > 0) {
        LOG.debug("Sleeping to let the interpreter boot up in memory");
        Thread.sleep(sleepTimeAfterInterpreterStart);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /***
   * Used to execute all of the commands from the command history when an operator is instantiating a new instance of
   *  the engine. Used by the dynamic partitioner to let a newly provisioned operator to catch up to the state of all of
   *  the remaining operator instances
   * @throws ApexPythonInterpreterException
   */
  @Override
  public void postStartInterpreter() throws ApexPythonInterpreterException
  {
    for ( InterpreterWrapper wrapper : workers) {
      for (PythonRequestResponse requestResponse : commandHistory) {
        PythonInterpreterRequest requestPayload = requestResponse.getPythonInterpreterRequest();
        try {
          wrapper.processRequest(requestResponse,requestPayload);
        } catch (InterruptedException e) {
          throw new ApexPythonInterpreterException(e);
        }
      }
    }
  }

  /***
   * See {@link ApexPythonEngine#runCommands(WorkerExecutionMode, long, long, PythonInterpreterRequest)} for more
   *  details. Note that if the worker execution mode {@link WorkerExecutionMode} is BROADCAST, then the time SLA
   *  set is the total time for all workers i.e. each worker is given a ( total time / N ) where N is the current
   *   number of worker threads
   * @param executionMode Whether these commands need to be run on all worker nodes or any of the worker node
   * @param windowId used to select the worker from the worker pool.Can be any long if an operator is not using this.
   * @param requestId used to select the worker from the worker pool. Can be any long if an operator is not using this.
   * @param request Represents the request to be processed.
   * @return A map containing the command as key and boolean representing success or failure as the value.
   * @throws ApexPythonInterpreterException
   */
  @Override
  public Map<String,PythonRequestResponse<Void>> runCommands(WorkerExecutionMode executionMode,long windowId,
      long requestId, PythonInterpreterRequest<Void> request) throws ApexPythonInterpreterException
  {
    checkNotNullConditions(request);
    checkNotNull(request.getGenericCommandsRequestPayload(), "Run commands payload not set");
    checkNotNull(request.getGenericCommandsRequestPayload().getGenericCommands(),
        "Commands that need to be run not set");
    Map<String,PythonRequestResponse<Void>> returnStatus = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    try {
      if (executionMode.equals(WorkerExecutionMode.BROADCAST)) {
        LOG.debug("Executing run commands on all of the interpreter worker threads");
        long  timeOutPerWorker = TimeUnit.NANOSECONDS.convert(request.getTimeout(),request.getTimeUnit()) /
            numWorkerThreads;
        LOG.debug("Allocating " + timeOutPerWorker + " nanoseconds for each of the worker threads");
        if ( timeOutPerWorker == 0) {
          timeOutPerWorker = 1;
        }
        request.setTimeout(timeOutPerWorker);
        request.setTimeUnit(TimeUnit.NANOSECONDS);
        for ( InterpreterWrapper wrapper : workers) {
          lastSuccessfullySubmittedRequest = wrapper.runCommands(windowId,requestId,request);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(wrapper.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        }
        if ( returnStatus.size() > 0) {
          commandHistory.add(lastSuccessfullySubmittedRequest);
        }
      } else {
        InterpreterWrapper currentThread = null;
        if (executionMode.equals(WorkerExecutionMode.ANY)) {
          LOG.debug("Executing run commands on a single interpreter worker thread");
          currentThread = selectWorkerForCurrentCall(requestId);
        }
        if (executionMode.equals(WorkerExecutionMode.STICKY)) {
          currentThread = workers.get(request.hashCode() % numWorkerThreads);
          LOG.debug(" Choosing sticky worker " + currentThread.getInterpreterId());
        }
        if (currentThread != null) {
          lastSuccessfullySubmittedRequest = currentThread.runCommands(windowId, requestId, request);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(currentThread.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        } else {
          throw new ApexPythonInterpreterException("No free interpreter threads available." +
            " Consider increasing workers and relaunch");
        }
      }
    } catch (InterruptedException e) {
      throw new ApexPythonInterpreterException(e);
    }
    return returnStatus;
  }

  /***
   *  See {@link ApexPythonEngine#executeMethodCall(WorkerExecutionMode, long, long, PythonInterpreterRequest)} for more
   *  details. Note that if the worker execution mode {@link WorkerExecutionMode} is BROADCAST, then the time SLA
   *  set is the total time for all workers i.e. each worker is given a ( total time / N ) where N is the current
   *   number of worker threads
   *
   * @param executionMode If the method call needs to be invoked on all workers or any single worker
   * @param windowId used to select the worker from the worker pool.Can be any long if an operator is not using this.
   * @param requestId used to select the worker from the worker pool. Can be any long if an operator is not using this.
   * @param req Represents the request to be processed.
   * @param <T>
   * @return
   * @throws ApexPythonInterpreterException
   */
  @Override
  public <T> Map<String,PythonRequestResponse<T>> executeMethodCall(WorkerExecutionMode executionMode,long windowId,
      long requestId, PythonInterpreterRequest<T> req) throws ApexPythonInterpreterException
  {
    checkNotNullConditions(req);
    checkNotNull(req.getMethodCallRequest(), "Method call info not set");
    checkNotNull(req.getMethodCallRequest().getNameOfMethod(), "Method name not set");
    Map<String,PythonRequestResponse<T>> returnStatus = new HashMap<>();
    req.setCommandType(PythonCommandType.METHOD_INVOCATION_COMMAND);
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    try {
      if (executionMode.equals(WorkerExecutionMode.BROADCAST)) {
        long timeOutPerWorker = TimeUnit.NANOSECONDS.convert(req.getTimeout(), req.getTimeUnit()) /
            numWorkerThreads;
        if ( timeOutPerWorker == 0) {
          timeOutPerWorker = 1;
        }
        req.setTimeout(timeOutPerWorker);
        req.setTimeUnit(TimeUnit.NANOSECONDS);
        for ( InterpreterWrapper wrapper : workers) {
          lastSuccessfullySubmittedRequest = wrapper.executeMethodCall(windowId,requestId,req);
          if ( lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(wrapper.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        }
        if ( returnStatus.size() > 0) {
          commandHistory.add(lastSuccessfullySubmittedRequest);
        }
      } else {
        InterpreterWrapper currentThread = null;
        if (executionMode.equals(WorkerExecutionMode.ANY)) {
          currentThread = selectWorkerForCurrentCall(requestId);
        }
        if (executionMode.equals(WorkerExecutionMode.STICKY)) {
          currentThread = workers.get(req.hashCode() % numWorkerThreads);
          LOG.debug(" Choosing sticky worker " + currentThread.getInterpreterId());
        }
        if (currentThread != null) {
          lastSuccessfullySubmittedRequest = currentThread.executeMethodCall(windowId, requestId, req);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(currentThread.getInterpreterId(), lastSuccessfullySubmittedRequest);
          } else {
            throw new ApexPythonInterpreterException("No free interpreter threads available." +
              " Consider increasing workers and relaunch");
          }
        }
      }
    } catch (InterruptedException e) {
      throw new ApexPythonInterpreterException(e);
    }
    return returnStatus;
  }

  /***
   *   See {@link ApexPythonEngine#executeScript(WorkerExecutionMode, long, long, PythonInterpreterRequest)} for more
   *  details. Note that if the worker execution mode {@link WorkerExecutionMode} is BROADCAST, then the time SLA
   *  set is the total time for all workers i.e. each worker is given a ( total time / N ) where N is the current
   *   number of worker threads
   * @param executionMode  If the method call needs to be invoked on all workers or any single worker
   * @param windowId used to select the worker from the worker pool.Can be any long if an operator is not using this.
   * @param requestId used to select the worker from the worker pool. Can be any long if an operator is not using this.
   * @param req Represents the request to be processed.
   * @return
   * @throws ApexPythonInterpreterException
   */
  @Override
  public Map<String,PythonRequestResponse<Void>> executeScript(WorkerExecutionMode executionMode,long windowId,
      long requestId, PythonInterpreterRequest<Void> req)
    throws ApexPythonInterpreterException
  {
    checkNotNullConditions(req);
    checkNotNull(req.getScriptExecutionRequestPayload(), "Script execution info not set");
    checkNotNull(req.getScriptExecutionRequestPayload().getScriptName(), "Script name not set");
    Map<String,PythonRequestResponse<Void>> returnStatus = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    try {
      if (executionMode.equals(WorkerExecutionMode.BROADCAST)) {
        long timeOutPerWorker = TimeUnit.NANOSECONDS.convert(req.getTimeout(), req.getTimeUnit()) /
            numWorkerThreads;
        if ( timeOutPerWorker == 0) {
          timeOutPerWorker = 1;
        }
        req.setTimeout(timeOutPerWorker);
        req.setTimeUnit(TimeUnit.NANOSECONDS);
        for ( InterpreterWrapper wrapper : workers) {
          lastSuccessfullySubmittedRequest = wrapper.executeScript(windowId,requestId,req);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(wrapper.getInterpreterId(),lastSuccessfullySubmittedRequest);
          }
        }
        if ( returnStatus.size() > 0) {
          commandHistory.add(lastSuccessfullySubmittedRequest);
        }
      } else {
        InterpreterWrapper currentThread = null;
        if (executionMode.equals(WorkerExecutionMode.ANY)) {
          currentThread = selectWorkerForCurrentCall(requestId);
        }
        if (executionMode.equals(WorkerExecutionMode.STICKY)) {
          currentThread = workers.get(req.hashCode() % numWorkerThreads);
          LOG.debug(" Choosing sticky worker " + currentThread.getInterpreterId());
        }
        if (currentThread != null) {
          lastSuccessfullySubmittedRequest = currentThread.executeScript(windowId, requestId, req);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(currentThread.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        } else {
          throw new ApexPythonInterpreterException("No free interpreter threads available." +
            " Consider increasing workers and relaunch");
        }
      }
    } catch (InterruptedException e) {
      throw new ApexPythonInterpreterException(e);
    }
    return returnStatus;
  }

  private void checkNotNullConditions(PythonInterpreterRequest request)
  {
    checkNotNull(request, "Request object cannnot be null");
    checkNotNull(request.getTimeout(), "Time out value not set");
    checkNotNull(request.getTimeUnit(), "Time out unit not set");
  }

  /***
   *  See {@link ApexPythonEngine#eval(WorkerExecutionMode, long, long, PythonInterpreterRequest)} for more
   *  details. Note that if the worker execution mode {@link WorkerExecutionMode} is BROADCAST, then the time SLA
   *  set is the total time for all workers i.e. each worker is given a ( total time / N ) where N is the current
   *   number of worker threads
   * @param executionMode If the method call needs to be invoked on all workers or any single worker
   * @param windowId used to select the worker from the worker pool.Can be any long if an operator is not using this.
   * @param requestId used to select the worker from the worker pool. Can be any long if an operator is not using this.
   * @param request
   * @param <T>
   * @return
   * @throws ApexPythonInterpreterException
   */
  @Override
  public <T> Map<String,PythonRequestResponse<T>> eval(WorkerExecutionMode executionMode,long windowId, long requestId,
      PythonInterpreterRequest<T> request) throws ApexPythonInterpreterException
  {
    checkNotNullConditions(request);
    checkNotNull(request.getEvalCommandRequestPayload(), "Eval command info not set");
    checkNotNull(request.getEvalCommandRequestPayload().getEvalCommand(),"Eval command not set");
    Map<String,PythonRequestResponse<T>> statusOfEval = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    try {
      if (executionMode.equals(WorkerExecutionMode.BROADCAST)) {
        long timeOutPerWorker = TimeUnit.NANOSECONDS.convert(request.getTimeout(), request.getTimeUnit()) /
            numWorkerThreads;
        if ( timeOutPerWorker == 0) {
          timeOutPerWorker = 1;
        }
        request.setTimeout(timeOutPerWorker);
        request.setTimeUnit(TimeUnit.NANOSECONDS);
        for ( InterpreterWrapper wrapper : workers) {
          lastSuccessfullySubmittedRequest = wrapper.eval(windowId,requestId,request);
          if (lastSuccessfullySubmittedRequest != null) {
            statusOfEval.put(wrapper.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        }
        commandHistory.add(lastSuccessfullySubmittedRequest);
      } else {
        InterpreterWrapper currentThread = null;
        if (executionMode.equals(WorkerExecutionMode.ANY)) {
          currentThread = selectWorkerForCurrentCall(requestId);
        }
        if (executionMode.equals(WorkerExecutionMode.STICKY)) {
          currentThread = workers.get(request.hashCode() % numWorkerThreads);
          LOG.debug(" Choosing sticky worker " + currentThread.getInterpreterId());
        }
        if (currentThread != null) {
          lastSuccessfullySubmittedRequest = currentThread.eval(windowId, requestId, request);
          if (lastSuccessfullySubmittedRequest != null) {
            statusOfEval.put(currentThread.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        } else {
          throw new ApexPythonInterpreterException("No free interpreter threads available." +
            " Consider increasing workers and relaunch");
        }
      }
    } catch (InterruptedException e) {
      throw new ApexPythonInterpreterException(e);
    }
    return statusOfEval;
  }

  @Override
  public void stopInterpreter() throws ApexPythonInterpreterException
  {
    for ( InterpreterWrapper wrapper : workers) {
      wrapper.stopInterpreter();
    }
  }

  public int getNumWorkerThreads()
  {
    return numWorkerThreads;
  }

  public void setNumWorkerThreads(int numWorkerThreads)
  {
    this.numWorkerThreads = numWorkerThreads;
  }

  public List<InterpreterWrapper> getWorkers()
  {
    return workers;
  }

  public void setWorkers(List<InterpreterWrapper> workers)
  {
    this.workers = workers;
  }

  @Override
  public List<PythonRequestResponse> getCommandHistory()
  {
    return commandHistory;
  }

  @Override
  public void setCommandHistory(List<PythonRequestResponse> commandHistory)
  {
    this.commandHistory = commandHistory;
  }

  public long getSleepTimeAfterInterpreterStart()
  {
    return sleepTimeAfterInterpreterStart;
  }

  public void setSleepTimeAfterInterpreterStart(long sleepTimeAfterInterpreterStart)
  {
    this.sleepTimeAfterInterpreterStart = sleepTimeAfterInterpreterStart;
  }

  @Override
  public BlockingQueue<PythonRequestResponse> getDelayedResponseQueue()
  {
    return delayedResponseQueue;
  }

  @Override
  public void setDelayedResponseQueue(BlockingQueue<PythonRequestResponse> delayedResponseQueue)
  {
    this.delayedResponseQueue = delayedResponseQueue;
  }

  public SpinPolicy getCpuSpinPolicyForWaitingInBuffer()
  {
    return cpuSpinPolicyForWaitingInBuffer;
  }

  public void setCpuSpinPolicyForWaitingInBuffer(SpinPolicy cpuSpinPolicyForWaitingInBuffer)
  {
    this.cpuSpinPolicyForWaitingInBuffer = cpuSpinPolicyForWaitingInBuffer;
  }

  public int getBufferCapacity()
  {
    return bufferCapacity;
  }

  public void setBufferCapacity(int bufferCapacity)
  {
    this.bufferCapacity = bufferCapacity;
  }

  @Override
  public long getNumStarvedReturns()
  {
    return numStarvedReturns;
  }

  @Override
  public void setNumStarvedReturns(long numStarvedReturns)
  {
    this.numStarvedReturns = numStarvedReturns;
  }
}

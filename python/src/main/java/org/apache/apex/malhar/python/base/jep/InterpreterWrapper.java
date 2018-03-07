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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonInterpreterConfig;
import org.apache.apex.malhar.python.base.requestresponse.PythonCommandType;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterResponse;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

/***
 * Wraps around the interpreter thread so that time bound SLAs can be implemented for python based execution
 * This class primarily implements the time constraints by utilizing the {@link InterpreterThread} class and using
 *  a Disruptor blocking queue for high throughput. Utilizes an executor service to implement the timing SLAs.
 */
public class InterpreterWrapper
{
  private static final Logger LOG = LoggerFactory.getLogger(InterpreterWrapper.class);

  /* Reference to the interpreter thread which executes requests in memory  */
  private transient InterpreterThread interpreterThread;

  /* Spin policy to use  for the disruptor implementation */
  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 16; // Represents the number of workers and response queue sizes

  private String interpreterId;

  /* Represents the actual thread instance running under the Executor service  */
  private transient Future<?> handleToJepRunner;

  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  private transient BlockingQueue<PythonRequestResponse> requestQueue;
  private transient BlockingQueue<PythonRequestResponse> responseQueue;

  /* Represents the queue into which all of the stragglers will be pushed into by the interpreter thread */
  private transient volatile BlockingQueue<PythonRequestResponse> delayedResponsesQueue;

  /***
   * Constructs the interpreter wrapper instance.
   * @param interpreterId A string that can be used to represent the interpreter id that is passed onto the actual
   *                      thread that is executing the commands
   *
   * @param delayedResponsesQueueRef The queue into which all of the straggler responses will end in
   */
  public InterpreterWrapper(String interpreterId,BlockingQueue<PythonRequestResponse> delayedResponsesQueueRef,
      SpinPolicy spinPolicyForWaitingInRequestQueue)
  {
    delayedResponsesQueue = delayedResponsesQueueRef;
    this.interpreterId = interpreterId;
    this.cpuSpinPolicyForWaitingInBuffer = spinPolicyForWaitingInRequestQueue;
    requestQueue = new DisruptorBlockingQueue<>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);
    responseQueue =  new DisruptorBlockingQueue<>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);
    interpreterThread = new InterpreterThread(requestQueue,responseQueue,interpreterId);
  }

  /**
   * Invokes the interpreter thread pre initialization logic
   * @param preInitConfigs A set of key value pairs that are used to initialize the actual interpreter. See constants
   *                       defined in {@link InterpreterThread} for a list of keys available
   * @throws ApexPythonInterpreterException if the pre-initialization logic could not be executed for whatever reasons
   */
  public void preInitInterpreter(Map<PythonInterpreterConfig, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    interpreterThread.preInitInterpreter(preInitConfigs);
  }

  /***
   * Starts the actual interpreter thread to which this class is wrapping around by using an executor service
   * @throws ApexPythonInterpreterException
   */
  public void startInterpreter() throws ApexPythonInterpreterException
  {
    handleToJepRunner = executorService.submit(interpreterThread);
  }

  /***
   * Builds a response object for the incoming request object.
   * @param req Represents the incoming request for which response needs to be generated for.
   * @param windowId The Operator window ID ( used to choose the interpreter thread while choosing to execute the
   *                  logic from a pool of worker threads )
   * @param requestId The request ID perhaps coming from the base python operator. Only used to optimize the right
   *                  interpreter lookup from a pool of worker interpreter threads.
   * @param <T> The template of the return type
   * @return An object of type {@link PythonRequestResponse} is returned which encompasses both request and response.
   */
  private <T> PythonRequestResponse<T> buildRequestRespObject(PythonInterpreterRequest<T> req,
      long windowId,long requestId)
  {
    PythonRequestResponse<T> requestResponse = new PythonRequestResponse();
    requestResponse.setPythonInterpreterRequest(req);
    PythonInterpreterResponse<T> response = new PythonInterpreterResponse<>(req.getExpectedReturnType());
    requestResponse.setPythonInterpreterResponse(response);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(requestId);
    requestResponse.setWindowId(windowId);
    return requestResponse;
  }

  /***
   * Handles the common logic that is common across all methods of invocation of the in-memory interpreter. Some common
   *  logic includes draining any stragglers, matching the request to the any of the responses that arrive in the
   *   response queue possibly due to previous requests
   * @param requestResponse The wrapper object into which
   * @param req The request that contains the timeout SLAs
   * @param <T> Java templating signature
   * @return A response to the original incoming request, null if the response did not arrive within given SLA.
   * @throws InterruptedException if interrupted while waiting for the response queue.
   */
  public <T> PythonRequestResponse<T> processRequest(PythonRequestResponse requestResponse,
      PythonInterpreterRequest<T> req) throws InterruptedException
  {
    List<PythonRequestResponse> drainedResults = new ArrayList<>();
    PythonRequestResponse currentRequestWithResponse = null;
    boolean isCurrentRequestProcessed = false;
    long timeOutInNanos = TimeUnit.NANOSECONDS.convert(req.getTimeout(),req.getTimeUnit());
    // drain any previous responses that were returned while the Apex operator is processing
    responseQueue.drainTo(drainedResults);
    LOG.debug("Draining previous request responses if any " + drainedResults.size());
    for (PythonRequestResponse oldRequestResponse : drainedResults) {
      delayedResponsesQueue.put(oldRequestResponse);
    }
    // We first set a timer to see how long it actually it took for the response to arrive.
    // It is possible that a response arrived due to a previous request and hence this need for the timer
    // which tracks the time for the current request.
    long currentStart = System.nanoTime();
    long timeLeftToCompleteProcessing = timeOutInNanos;
    while ( (!isCurrentRequestProcessed) && ( timeLeftToCompleteProcessing > 0 )) {
      LOG.debug("Submitting the interpreter Request with time out in nanos as " + timeOutInNanos);
      requestQueue.put(requestResponse);
      // ensures we are blocked till the time limit
      currentRequestWithResponse = responseQueue.poll(timeOutInNanos, TimeUnit.NANOSECONDS);
      timeLeftToCompleteProcessing = timeLeftToCompleteProcessing - ( System.nanoTime() - currentStart );
      currentStart = System.nanoTime();
      if (currentRequestWithResponse != null) {
        if ( (requestResponse.getRequestId() == currentRequestWithResponse.getRequestId()) &&
            (requestResponse.getWindowId() == currentRequestWithResponse.getWindowId()) ) {
          isCurrentRequestProcessed = true;
          break;
        } else {
          delayedResponsesQueue.put(currentRequestWithResponse);
        }
      } else {
        LOG.debug(" Processing of request could not be completed on time");
      }
    }
    if (isCurrentRequestProcessed) {
      LOG.debug("Response could be processed within time SLA");
      return currentRequestWithResponse;
    } else {
      LOG.debug("Response could not be processed within time SLA");
      return null;
    }
  }

  /***
   * Implements the time based SLA over the interpreters run commands implementation. See
   *  {@link InterpreterThread#runCommands(List)}
   * @param windowId The window ID as provided by the Apex operator. Used for selecting a worker from the worker pool.
   * @param requestId The request ID as provided by the Apex operator. Used for selecting a worker from the worker pool.
   * @param request The payload of the request
   * @return A response object with the results of the execution. Null if the request could not be processed on time.
   * @throws InterruptedException if interrupted while processing the wait for request or writing to delayed response
   *  queue
   */
  public PythonRequestResponse<Void> runCommands(long windowId, long requestId,
      PythonInterpreterRequest<Void> request) throws InterruptedException
  {
    request.setCommandType(PythonCommandType.GENERIC_COMMANDS);
    PythonRequestResponse requestResponse = buildRequestRespObject(request,windowId,requestId);
    return processRequest(requestResponse,request);
  }

  /***
   * Implements the time based SLA over the interpreters run commands implementation. See
   *  {@link InterpreterThread#executeMethodCall(String, List, Class)}
   * @param windowId The window ID as provided by the Apex operator. Used for selecting a worker from the worker pool.
   * @param requestId The request ID as provided by the Apex operator. Used for selecting a worker from the worker pool.
   * @param request The payload of the request
   * @return A response object with the results of the execution. Null if the request could not be processed on time.
   * @throws InterruptedException if interrupted while processing the wait for request or writing to delayed response
   *  queue
   */
  public <T> PythonRequestResponse<T> executeMethodCall(long windowId, long requestId,
      PythonInterpreterRequest<T> request)  throws InterruptedException
  {
    request.setCommandType(PythonCommandType.METHOD_INVOCATION_COMMAND);
    PythonRequestResponse requestResponse = buildRequestRespObject(request, windowId,requestId);
    return processRequest(requestResponse,request);
  }

  /***
   * Implements the time based SLA over the interpreters run commands implementation. See
   *  {@link InterpreterThread#executeScript(String)}
   * @param windowId The window ID as provided by the Apex operator. Used for selecting a worker from the worker pool.
   * @param requestId The request ID as provided by the Apex operator. Used for selecting a worker from the worker pool.
   * @param request The payload of the request
   * @return A response object with the results of the execution. Null if the request could not be processed on time.
   * @throws InterruptedException if interrupted while processing the wait for request or writing to delayed response
   *  queue
   */
  public PythonRequestResponse<Void> executeScript(long windowId,long requestId,PythonInterpreterRequest<Void> request)
      throws InterruptedException
  {
    request.setCommandType(PythonCommandType.SCRIPT_COMMAND);
    PythonRequestResponse<Void> requestResponse = buildRequestRespObject(request, windowId,requestId);
    return processRequest(requestResponse,request);
  }


  /***
   * Implements the time based SLA over the interpreters run commands implementation. See
   *  {@link InterpreterThread#eval(String, String, Map, boolean, Class)}
   * @param windowId The window ID as provided by the Apex operator. Used for selecting a worker from the worker pool.
   * @param requestId The request ID as provided by the Apex operator. Used for selecting a worker from the worker pool.
   * @param request The payload of the request
   * @return A response object with the results of the execution. Null if the request could not be processed on time.
   * @throws InterruptedException if interrupted while processing the wait for request or writing to delayed response
   *  queue
   */
  public <T> PythonRequestResponse<T> eval(long windowId, long requestId,PythonInterpreterRequest<T> request)
      throws InterruptedException
  {
    request.setCommandType(PythonCommandType.EVAL_COMMAND);
    PythonRequestResponse<T> requestResponse = buildRequestRespObject(request,windowId,requestId);
    return processRequest(requestResponse,request);
  }

  /***
   * Stops the interpreter
   * @throws ApexPythonInterpreterException if error while stopping the interpreter
   */
  public void stopInterpreter() throws ApexPythonInterpreterException
  {
    interpreterThread.setStopped(true);
    handleToJepRunner.cancel(false);
    executorService.shutdown();
  }

  public InterpreterThread getInterpreterThread()
  {
    return interpreterThread;
  }

  public void setInterpreterThread(InterpreterThread interpreterThread)
  {
    this.interpreterThread = interpreterThread;
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

  public String getInterpreterId()
  {
    return interpreterId;
  }

  public void setInterpreterId(String interpreterId)
  {
    this.interpreterId = interpreterId;
  }

  public BlockingQueue<PythonRequestResponse> getRequestQueue()
  {
    return requestQueue;
  }

  public void setRequestQueue(BlockingQueue<PythonRequestResponse> requestQueue)
  {
    this.requestQueue = requestQueue;
  }

  public BlockingQueue<PythonRequestResponse> getResponseQueue()
  {
    return responseQueue;
  }

  public void setResponseQueue(BlockingQueue<PythonRequestResponse> responseQueue)
  {
    this.responseQueue = responseQueue;
  }

  public BlockingQueue<PythonRequestResponse> getDelayedResponsesQueue()
  {
    return delayedResponsesQueue;
  }

  public void setDelayedResponsesQueue(BlockingQueue<PythonRequestResponse> delayedResponsesQueue)
  {
    this.delayedResponsesQueue = delayedResponsesQueue;
  }

  public boolean isCurrentlyBusy()
  {
    return interpreterThread.isBusy();
  }


}

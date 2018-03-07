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
package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

/**
 * Defines the methods that needs to be implemented by the Python Engine implementations. The first implementation
 *  takes the approach of in-memory interpreter using JEP. Other possibilities are using Py4J which is an inter process
 *   communication model. An Apex operator would use an instance of the Python engine implementations to run
 * python code using the chosen engine.
 */
public interface ApexPythonEngine
{
  /***
   * Used to perform any pre interpreter processing.
   * @param preInitConfigs The configuration that is going to be used by the interpreter
   * @throws ApexPythonInterpreterException if there is an issue in executing the pre interpreter logic
   */
  void preInitInterpreter(Map<PythonInterpreterConfig,Object> preInitConfigs) throws ApexPythonInterpreterException;

  /***
   * Starts the interpreter.
   * @throws ApexPythonInterpreterException if library not locatable or any other issue starting the interpreter
   */
  void startInterpreter() throws ApexPythonInterpreterException;

  /***
   * Used to perform any logic that needs to be executed after the interpreter is started but before any tuples start
   *  getting processed. Example, setting the starting state of the variables that are used in tuple processing.
   * @throws ApexPythonInterpreterException
   */
  void postStartInterpreter() throws ApexPythonInterpreterException;

  /***
   * Runs a series of commands. The implementation engine could make use of a worker pool to execute the command.
   * @param executionMode Whether these commands need to be run on all worker thread or any of the worker thread.Please see
   *    *                       {@link WorkerExecutionMode} for choices available
   * @param windowId used to select the worker from the worker thread pool. This parameter gains significance if
   *                 we want to implement a sticky worker in the near future. This will allow for a basic approach to
   *                 route the command/s to the same worker if the application logic needs it to be. In the case of ANY
   *                 worker logic, the window ID along with the Request ID is used to implement a round robin approach
   *                 to select the next worker. Note that Sticky worker might be required since python interpreter
   *                 state is accumulated over as the commands run and a command can reference a variable created in a
   *                 previous command etc. Such references might want to route all commands to a specific interpreter
   *                 instance. If the Apex python engine is not being used by an operator implementation directly,
   *                 the caller can pass in any number as it is not used in anything more than selecting a worker from a
   *                 worker pool.
   * @param requestId The parameter is used to select a worker from the
   *                  worker pool along with the window Id. If the Apex python engine is not being used by an
   *                  operator implementation directly, the caller can pass in any number as it is not used in anything
   *                  more than selecting a worker from a worker pool.
   * @param request Represents the request to be processed.
   * @return A map with key as the command run and boolean as the value. True represents that the command successfully
   *  run.
   * @throws ApexPythonInterpreterException if interrupted or if the command cannot be executed
   */
  Map<String,PythonRequestResponse<Void>> runCommands(WorkerExecutionMode executionMode, long windowId, long requestId,
      PythonInterpreterRequest<Void> request) throws ApexPythonInterpreterException;

  /***
   * Executes a method call
   * @param executionMode If the method call needs to be invoked on all workers or any single worker. Please see
   *    *                       {@link WorkerExecutionMode} for choices available
   * @param windowId used to select the worker from the worker thread pool. This parameter gains significance if
   *                 we want to implement a sticky worker in the near future. This will allow for a basic approach to
   *                 route the command/s to the same worker if the application logic needs it to be. In the case of ANY
   *                 worker logic, the window ID along with the Request ID is used to implement a round robin approach
   *                 to select the next worker. Note that Sticky worker might be required since python interpreter
   *                 state is accumulated over as the commands run and a command can reference a variable created in a
   *                 previous command etc. Such references might want to route all commands to a specific interpreter
   *                 instance. If the Apex python engine is not being used by an operator implementation directly,
   *                 the caller can pass in any number as it is not used in anything more than selecting a worker from a
   *                 worker pool.
   * @param requestId The parameter is used to select a worker from the
   *                  worker pool along with the window Id. If the Apex python engine is not being used by an
   *                  operator implementation directly, the caller can pass in any number as it is not used in anything
   *                  more than selecting a worker from a worker pool.
   * @param req Represents the request to be processed.
   * @param <T>
   * @return A map containing the worker ID as key and boolean as successful or not
   * @throws ApexPythonInterpreterException
   */
  <T> Map<String,PythonRequestResponse<T>> executeMethodCall(WorkerExecutionMode executionMode,long windowId,
      long requestId, PythonInterpreterRequest<T> req) throws ApexPythonInterpreterException;

  /***
   * Executes a script that is locatable via a file path
   * @param executionMode  If the method call needs to be invoked on all workers or any single worker. Please see
   *                       {@link WorkerExecutionMode} for choices available
   * @param windowId used to select the worker from the worker thread pool. This parameter gains significance if
   *                 we want to implement a sticky worker in the near future. This will allow for a basic approach to
   *                 route the command/s to the same worker if the application logic needs it to be. In the case of ANY
   *                 worker logic, the window ID along with the Request ID is used to implement a round robin approach
   *                 to select the next worker. Note that Sticky worker might be required since python interpreter
   *                 state is accumulated over as the commands run and a command can reference a variable created in a
   *                 previous command etc. Such references might want to route all commands to a specific interpreter
   *                 instance. If the Apex python engine is not being used by an operator implementation directly,
   *                 the caller can pass in any number as it is not used in anything more than selecting a worker from a
   *                 worker pool.
   * @param requestId The parameter is used to select a worker from the
   *                  worker pool along with the window Id. If the Apex python engine is not being used by an
   *                  operator implementation directly, the caller can pass in any number as it is not used in anything
   *                  more than selecting a worker from a worker pool.
   * @param request Represents the request to be processed.
   * @return A map containing the worker ID as key and boolean as successful or not
   * @throws ApexPythonInterpreterException
   */
  Map<String,PythonRequestResponse<Void>>  executeScript(WorkerExecutionMode executionMode,long windowId,long requestId,
      PythonInterpreterRequest<Void> request) throws ApexPythonInterpreterException;

  /***
   * Evaluates a string as a python expression and also supports passing in variables from JVM to the python interpreter
   * @param executionMode If the method call needs to be invoked on all workers or any single worker. Please see
   *    *                       {@link WorkerExecutionMode} for choices available
   * @param windowId used to select the worker from the worker thread pool. This parameter gains significance if
   *                 we want to implement a sticky worker in the near future. This will allow for a basic approach to
   *                 route the command/s to the same worker if the application logic needs it to be. In the case of ANY
   *                 worker logic, the window ID along with the Request ID is used to implement a round robin approach
   *                 to select the next worker. Note that Sticky worker might be required since python interpreter
   *                 state is accumulated over as the commands run and a command can reference a variable created in a
   *                 previous command etc. Such references might want to route all commands to a specific interpreter
   *                 instance. If the Apex python engine is not being used by an operator implementation directly,
   *                 the caller can pass in any number as it is not used in anything more than selecting a worker from a
   *                 worker pool.
   * @param requestId The parameter is used to select a worker from the
   *                  worker pool along with the window Id. If the Apex python engine is not being used by an
   *                  operator implementation directly, the caller can pass in any number as it is not used in anything
   *                  more than selecting a worker from a worker pool.
   * @param req Represents the request to be processed.
   * @param <T> Java templating signature
   * @return A map containing the worker ID as key and boolean as successful or not
   * @throws ApexPythonInterpreterException
   */
  <T> Map<String,PythonRequestResponse<T>> eval(WorkerExecutionMode executionMode, long windowId, long requestId,
      PythonInterpreterRequest<T> req)  throws ApexPythonInterpreterException;

  /***
   * @return The queue that holds all of the straggler responses.
   */
  BlockingQueue<PythonRequestResponse> getDelayedResponseQueue();

  void setDelayedResponseQueue(BlockingQueue<PythonRequestResponse> delayedResponseQueue);

  /***
   * @return The number of times the engine could not process a request as there were no free worker threads and hence
   *  had to return null
   */
  long getNumStarvedReturns();


  void setNumStarvedReturns(long numStarvedReturns);

  /**
   * Returns all of the commands that were executed on all of the worker nodes.
   * @return History of all commands executed in sequence
   */
  List<PythonRequestResponse> getCommandHistory();

  void setCommandHistory(List<PythonRequestResponse> commandHistory);

  void stopInterpreter() throws ApexPythonInterpreterException;

}

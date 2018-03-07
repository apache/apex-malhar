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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonInterpreterConfig;
import org.apache.apex.malhar.python.base.requestresponse.EvalCommandRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.MethodCallRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterResponse;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;
import org.apache.apex.malhar.python.base.requestresponse.ScriptExecutionRequestPayload;
import org.apache.apex.malhar.python.base.util.NDimensionalArray;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;
import jep.NDArray;

import static org.apache.apex.malhar.python.base.PythonInterpreterConfig.IDLE_INTERPRETER_SPIN_POLICY;
import static org.apache.apex.malhar.python.base.PythonInterpreterConfig.PYTHON_INCLUDE_PATHS;
import static org.apache.apex.malhar.python.base.PythonInterpreterConfig.PYTHON_SHARED_LIBS;
import static org.apache.apex.malhar.python.base.PythonInterpreterConfig.SLEEP_TIME_MS_IN_CASE_OF_NO_REQUESTS;

/**
 * <p>
 * Represents a python interpreter instance embedded in JVM memory using the JEP ( Java embedded Python ) engine.
 * JEP uses JNI wrapper around the embedded Python instance. JEP mandates that the thread that created the JEP
 * instance is the only thread that can perform method calls on the embedded interpreter. This requires
 * the Apex operator implementation to decouple JEP execution logic from the operator processing main thread.
 * <b>Note that this embedded python is an interpreter and this essentially means the state of the interpreter
 * is maintained across all calls to the interpreter.</b>
 * The threaded implementation provides the following main functionalities
 * <ol>
 *   <li>An evaluation expression that can interpret a string as a python command. The user can also set
 *    variable values that are
 *      <ul>
 *         <li>Transferred to the interpreter with the same variable names</li>
 *         <li>Garbage collected from the python interpreter space</li>
 *      </ul>
 *   </li>
 *   <li>
 *     A method call invocation wherein parameters can be sent to the previously defined method (the method must have to
 *      be defined perhaps via an eval expression or a previous execute script call)
 *   </li>
 *   <li>A script call command that can execute a script. There is currently no support to pass params to scripts</li>
 *   <li>A handy mechanism to execute a series of commands. Note that this is a simple wrapper around the
 *    eval expression. The main difference here is that there are no user variables substitution used in
 *     this model. This is useful for statements like import ( ex: import numpy as np ) which does not require
 *      user variables conversion</li>
 * </ol>
 * </p>
 *
 * <p>
 *   The logic is executed using a request and response queue model. The thread keeps consuming from a request queue
 *   and submits results to a response queue.
 * </p>
 * <p>
 *   Note that all outputs are being redirected to the standard logger. Hence using statements like print(secret)
 *   needs to be avoided as the result of the print command is captured in the log.
 * </p>
 * <p>
 *   When using Cpython libraries like numpy, <b>ensure you first register numpy as a shared library</b> before using it
 *   in even import statements. Not doing so will result in very obscure errors.
 * </p>
 */

public class InterpreterThread implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(InterpreterThread.class);

  /* Name of the dynamically loaded JNI library */
  public static final String JEP_LIBRARY_NAME = "jep";

  /* The string command which will be used to delete python variables after they are used. */
  public static final String PYTHON_DEL_COMMAND = "del ";

  public transient Jep JEP_INSTANCE;

  /* Used by the operator thread or other threads to mark the stopping of processing of the interpreter command loop */
  private transient volatile boolean isStopped = false;

  /* Used to represent the current state of this thread whether it is currently busy executing a command */
  private transient volatile boolean busyFlag = false;

  /* Represents the default amount of time that this thread will wait to read a command from the request queue */
  private long timeOutToPollFromRequestQueue = 1;

  private TimeUnit timeUnitsToPollFromRequestQueue = TimeUnit.MILLISECONDS;

  private transient volatile BlockingQueue<PythonRequestResponse> requestQueue;

  private transient volatile BlockingQueue<PythonRequestResponse> responseQueue;

  /* An id that can be useful while logging statements */
  private String threadID;

  /* Whether this thread should sleep for a few moments if there are no requests are keep checking the request queue */
  private SpinPolicy spinPolicy = SpinPolicy.SLEEP;

  /* Holds the configs that are used to initialize the interpreter thread. Examples of config are shared libraries and
  include paths for the interpreter. The key is one of the constants defined in PythonInterpreterConfig and value
    is specific to the config type that is being set.
   */
  private Map<PythonInterpreterConfig,Object> initConfigs = new HashMap<>();

  /* Used as a flag to denote an error situation in the interpreter so that the next set of commands to run
   *  an empty/null eval expression to clear any erraneous state  */
  private boolean errorEncountered = false;

  private long sleepTimeMsInCaseOfNoRequests = 1;

  /***
   * Constructs an interpreter thread instance. Note that the constructor does not start the interpreter in memory yet.
   * @param requestQueue The queue from which requests will be processed from.
   * @param responseQueue The queue into which the responses will be written into
   * @param threadID An identifier for this thread name for efficient logging markers
   */
  public InterpreterThread(BlockingQueue<PythonRequestResponse> requestQueue,
      BlockingQueue<PythonRequestResponse> responseQueue,String threadID)
  {
    this.requestQueue = requestQueue;
    this.responseQueue = responseQueue;
    this.threadID = threadID;
  }

  /***
   * Loads the JEP dynamic library for the JVM to use the JNI bridge into the interpreter
   * @throws ApexPythonInterpreterException if the library could not be loaded or located
   */
  private void loadMandatoryJVMLibraries() throws ApexPythonInterpreterException
  {
    LOG.info("Java library path being used for Interpreted ID " +  threadID + " " +
        System.getProperty("java.library.path"));
    try {
      System.loadLibrary(JEP_LIBRARY_NAME);
    } catch (Exception e) {
      throw new ApexPythonInterpreterException(e);
    }
    LOG.info("JEP library loaded successfully");
  }


  public Jep getEngineReference() throws ApexPythonInterpreterException
  {
    return JEP_INSTANCE;
  }

  /***
   * Executes the logic required before the start of the interpreter. In this case, it is just registering of the
   * configs which are to be used when the interpreter is about to load
   * @param preInitConfigs
   * @throws ApexPythonInterpreterException
   */
  public void preInitInterpreter(Map<PythonInterpreterConfig, Object> preInitConfigs)
    throws ApexPythonInterpreterException
  {
    initConfigs.putAll(preInitConfigs);
  }

  /***
   * Starts the interpreter by loading the shared libraries
   * @throws ApexPythonInterpreterException if the interpreter could not be started
   */
  public void startInterpreter() throws ApexPythonInterpreterException
  {
    Thread.currentThread().setName(threadID);
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY); // To allow for time aware calls
    loadMandatoryJVMLibraries();
    JepConfig config = new JepConfig()
        .setRedirectOutputStreams(true)
        .setInteractive(false)
        .setClassLoader(Thread.currentThread().getContextClassLoader()
    );
    if (initConfigs.containsKey(PYTHON_INCLUDE_PATHS)) {
      List<String> includePaths = (List<String>)initConfigs.get(PYTHON_INCLUDE_PATHS);
      if ( includePaths != null) {
        LOG.info("Adding include path for the in-memory interpreter instance");
        for (String anIncludePath: includePaths) {
          config.addIncludePaths(anIncludePath);
        }
      }
    }
    if (initConfigs.containsKey(PYTHON_SHARED_LIBS)) {
      Set<String> sharedLibs = (Set<String>)initConfigs.get(PYTHON_SHARED_LIBS);
      if ( sharedLibs != null) {
        config.setSharedModules(sharedLibs);
        LOG.info("Loaded " + sharedLibs.size() + " shared libraries as config");
      }
    } else {
      LOG.info(" No shared libraries loaded");
    }
    if (initConfigs.containsKey(IDLE_INTERPRETER_SPIN_POLICY)) {
      spinPolicy = SpinPolicy.valueOf((String)initConfigs.get(IDLE_INTERPRETER_SPIN_POLICY));
      LOG.debug("Configuring spin policy to be " + spinPolicy);
    }
    if (initConfigs.containsKey(SLEEP_TIME_MS_IN_CASE_OF_NO_REQUESTS)) {
      sleepTimeMsInCaseOfNoRequests = (Long)initConfigs.get(SLEEP_TIME_MS_IN_CASE_OF_NO_REQUESTS);
      LOG.debug("Configuring sleep time for no requests situation to be " + sleepTimeMsInCaseOfNoRequests);
    }
    try {
      LOG.info("Launching the in-memory interpreter");
      JEP_INSTANCE = new Jep(config);
    } catch (JepException e) {
      LOG.error(e.getMessage(),e); // Purposefully logging as this will help in startup issues being captured inline
      throw new ApexPythonInterpreterException(e);
    }
  }

  /***
   * Runs a series of interpreter commands. Note that no params can be passed from the JVM to the python interpreter
   * space
   * @param commands The series of commands that will be executed sequentially
   * @return A map containing the result of execution of each of the commands. The command is the key that was
   * passed as input and the value is a boolean whether the command was executed successfully
   */
  private Map<String,Boolean> runCommands(List<String> commands)
  {
    LOG.debug("Executing run commands");
    Map<String,Boolean> resultsOfExecution = new HashMap<>();
    for (String aCommand : commands) {
      LOG.debug("Executing command " + aCommand);
      try {
        resultsOfExecution.put(aCommand,JEP_INSTANCE.eval(aCommand));
      } catch (JepException e) {
        resultsOfExecution.put(aCommand,Boolean.FALSE);
        errorEncountered = true;
        LOG.error("Error while running command " + aCommand, e);
        return resultsOfExecution;
      }
    }
    return resultsOfExecution;
  }

  /***
   * Executes a method call by passing any parameters to the method call. The params are passed in the order they are
   *  set in the list.
   * @param nameOfGlobalMethod Name of the method to invoke
   * @param argsToGlobalMethod Arguments to the method call. Typecasting is interpreted at runtime and hence multiple
   *                           types can be sent as part of the parameter list
   * @param type The class of the return parameter. Note that in some cases the return type will be the highest possible
   *             bit size. For example addition of tow ints passed in might return a Long by the interpreter.
   * @param <T> Represents the type of the return parameter
   * @return The response from the method call that the python method returned
   */
  private <T> T executeMethodCall(String nameOfGlobalMethod, List<Object> argsToGlobalMethod, Class<T> type)
  {
    LOG.debug("Executing method call invocation");
    try {
      if ((argsToGlobalMethod != null) && (argsToGlobalMethod.size() > 0)) {
        List<Object> paramsToPass = argsToGlobalMethod;
        List<Object> modifiedParams = new ArrayList<>();
        for ( Object aMethodParam: argsToGlobalMethod) {
          if (argsToGlobalMethod.get(0) instanceof NDimensionalArray) {
            LOG.debug(aMethodParam + " is of type NDimensional array and hence converting to JEP NDArray");
            modifiedParams.add(((NDimensionalArray)aMethodParam).toNDArray());
          } else {
            modifiedParams.add(aMethodParam);
          }
        }
        LOG.debug("Executing method" + nameOfGlobalMethod + " with " + modifiedParams.size() + " parameters");
        return type.cast(JEP_INSTANCE.invoke(nameOfGlobalMethod,modifiedParams.toArray()));
      } else {
        LOG.debug("Executing " + argsToGlobalMethod + " with no parameters");
        return type.cast(JEP_INSTANCE.invoke(nameOfGlobalMethod,new ArrayList<>().toArray()));
      }
    } catch (JepException e) {
      errorEncountered = true;
      LOG.error("Error while executing method " + nameOfGlobalMethod, e);
    }
    return null;
  }

  /***
   * Executes a python script which can be located in the path
   * @param pathToScript The path to the script
   * @return true if the script invocation was successfull or false otherwise
   */
  private boolean executeScript(String pathToScript)
  {
    LOG.debug("Executing script at path " + pathToScript);
    try {
      JEP_INSTANCE.runScript(pathToScript);
      return true;
    } catch (JepException e) {
      errorEncountered = true;
      LOG.error(" Error while executing script " + pathToScript, e);
    }
    return false;
  }

  /***
   * Evaluates a string expression by passing in any variable subsitution into the Interpreter space if required. Also
   * handles the garbage collection of the variables passed and offers a configurable way to delete any variable created
   *  as part of the evaluation expression.
   * @param command The string equivalent of the command
   * @param variableToExtract The name of the variable that would need to be extracted from the python interpreter space
   *                          to the JVM space.
   * @param variableSubstituionParams Key value pairs representing the variables that need to be passed into the
   *                                  interpreter space and are part of the eval expression.
   * @param deleteExtractedVariable if the L.H.S. of an assignment expression variable needs to be deleted. This is
   *                                essentially the variable that is being requested to extract i.e. the second
   *                                parameter to this method.
   * @param expectedReturnType Class representing the expected return type
   * @param <T> Template signature for the expected return type
   * @return The value that is extracted from the interpreter space ( possibly created as part of the eval expression or
   *  otherwise ). Returns null if any error
   */
  private <T> T eval(String command, String variableToExtract, Map<String, Object> variableSubstituionParams,
      boolean deleteExtractedVariable,Class<T> expectedReturnType)
  {
    T variableToReturn = null;
    LOG.debug("Executing eval expression " + command + " with return type : " + expectedReturnType);
    try {
      for (String aKey : variableSubstituionParams.keySet()) {
        Object keyVal = variableSubstituionParams.get(aKey);
        if (keyVal instanceof NDimensionalArray) {
          keyVal = ((NDimensionalArray)keyVal).toNDArray();
        }
        JEP_INSTANCE.set(aKey, keyVal);
      }
    } catch (JepException e) {
      errorEncountered = true;
      LOG.error("Error while setting the params for eval expression " + command, e);
      return null;
    }
    try {
      LOG.debug("Executing the eval expression in the interpreter instance " + command);
      JEP_INSTANCE.eval(command);
    } catch (JepException e) {
      errorEncountered = true;
      LOG.error("Error while evaluating the expression " + command, e);
      return null;
    }
    try {
      if (variableToExtract != null) {
        Object extractedVariable = JEP_INSTANCE.getValue(variableToExtract);
        if (extractedVariable instanceof NDArray) {
          LOG.debug(" Return type is a NumPy Array. Hence converting to NDimensionalArray instance");
          NDArray ndArrayJepVal = (NDArray)extractedVariable;
          NDimensionalArray nDimArray = new NDimensionalArray();
          nDimArray.setData(ndArrayJepVal.getData());
          nDimArray.setSignedFlag(ndArrayJepVal.isUnsigned());
          int[] dimensions = ndArrayJepVal.getDimensions();
          nDimArray.setDimensions(dimensions);
          int lengthInOneDimension = 1;
          for ( int i = 0; i < dimensions.length; i++) {
            lengthInOneDimension *= dimensions[i];
          }
          nDimArray.setLengthOfSequentialArray(lengthInOneDimension);
          variableToReturn = expectedReturnType.cast(nDimArray);
        } else {
          variableToReturn =  expectedReturnType.cast(extractedVariable);
        }
        if (deleteExtractedVariable) {
          LOG.debug("Deleting the extracted variable from the Python interpreter space");
          JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + variableToExtract);
        }
      }
      LOG.debug("Deleting all the variables from the python interpreter space ");
      for (String aKey: variableSubstituionParams.keySet()) {
        LOG.debug("Deleting " + aKey);
        JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + aKey);
      }
    } catch (JepException e) {
      errorEncountered = true;
      LOG.error("Error while evaluating delete part of expression " + command, e);
      return null;
    }
    return variableToReturn;
  }

  /***
   * Stops the interpreter as requested from the operator/main thread
   * @throws ApexPythonInterpreterException if not able to stop the
   */
  public void stopInterpreter() throws ApexPythonInterpreterException
  {
    isStopped = true;
    LOG.info("Attempting to close the interpreter thread");
    try {
      JEP_INSTANCE.close();
    } catch (Exception e) {
      LOG.error("Error while stopping the interpreter thread ", e);
      throw new ApexPythonInterpreterException(e);
    }
    LOG.info("Interpreter closed");
  }

  /***
   * Responsible for polling the request queue and formatting the request payload to make it compatible to the
   *  individual processing logic of the functionalities provided by the interpreter API methods.
   * @param <T> Java templating signature enforcement
   * @throws ApexPythonInterpreterException if an unrecognized command is issued.
   * @throws InterruptedException if interrupted while trying to wait for a request from request queue
   */
  private <T> void processCommand() throws ApexPythonInterpreterException, InterruptedException
  {

    PythonRequestResponse requestResponseHandle = requestQueue.poll(timeOutToPollFromRequestQueue,
        timeUnitsToPollFromRequestQueue);
    if (requestResponseHandle != null) {
      LOG.debug("Processing command " + requestResponseHandle.getPythonInterpreterRequest().getCommandType());
      busyFlag = true;
      if (errorEncountered) {
        LOG.debug("Error state detected from a previous command. Resetting state to  the previous" +
            " state of the error");
        try {
          JEP_INSTANCE.eval(null);
          errorEncountered = false;
        } catch (JepException e) {
          LOG.error("Error while trying to clear the state of the interpreter due to previous command" +
              " " + e.getMessage(), e);
        }
      }
      PythonInterpreterRequest<T> request =
          requestResponseHandle.getPythonInterpreterRequest();
      PythonInterpreterResponse<T> response =
          requestResponseHandle.getPythonInterpreterResponse();
      Map<String,Boolean> commandStatus = new HashMap<>(1);
      switch (request.getCommandType()) {
        case EVAL_COMMAND:
          EvalCommandRequestPayload evalPayload = request.getEvalCommandRequestPayload();
          T responseVal = eval(evalPayload.getEvalCommand(), evalPayload.getVariableNameToExtractInEvalCall(),
              evalPayload.getParamsForEvalCommand(), evalPayload.isDeleteVariableAfterEvalCall(),
              request.getExpectedReturnType());
          response.setResponse(responseVal);
          if (responseVal != null) {
            commandStatus.put(evalPayload.getEvalCommand(),Boolean.TRUE);
          } else {
            commandStatus.put(evalPayload.getEvalCommand(),Boolean.FALSE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case SCRIPT_COMMAND:
          ScriptExecutionRequestPayload scriptPayload = request.getScriptExecutionRequestPayload();
          if (executeScript(scriptPayload.getScriptName())) {
            commandStatus.put(scriptPayload.getScriptName(),Boolean.TRUE);
          } else {
            commandStatus.put(scriptPayload.getScriptName(),Boolean.FALSE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case METHOD_INVOCATION_COMMAND:
          MethodCallRequestPayload requestpayload = request.getMethodCallRequest();
          response.setResponse(executeMethodCall(
              requestpayload.getNameOfMethod(), requestpayload.getArgs(), request.getExpectedReturnType()));
          if (response.getResponse() == null) {
            commandStatus.put(requestpayload.getNameOfMethod(), Boolean.FALSE);
          } else {
            commandStatus.put(requestpayload.getNameOfMethod(), Boolean.TRUE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case GENERIC_COMMANDS:
          response.setCommandStatus(runCommands(request.getGenericCommandsRequestPayload().getGenericCommands()));
          break;
        default:
          throw new ApexPythonInterpreterException(new Exception("Unspecified Interpreter command"));
      }
      requestResponseHandle.setRequestCompletionTime(System.currentTimeMillis());
      responseQueue.put(requestResponseHandle);
      LOG.debug("Submitted the response and executed " + response.getCommandStatus().size() + " instances of command");
    }
    busyFlag = false;
  }

  /***
   * Starts the interpreter as soon as the thread starts running. This is due to the limitation of JEP which stipulates
   *  that the thread which started the interpreter can only issue subsequent calls/invocations. This is due to JNI
   *   limitations. The thread then tries to consume from the request queue and process them. If there are no requests
   *    present then the thread can possibly go to sleep based on the {@link SpinPolicy} configured. The spin policy
   *     is passed in as the pre init configurations. See {@link PythonInterpreterConfig} for more details
   */
  @Override
  public void run()
  {
    LOG.info("Starting the execution of Interpreter thread ");
    if (JEP_INSTANCE == null) {
      LOG.info("Initializaing the interpreter state");
      startInterpreter();
      LOG.info("Successfully initialized the interpreter");
    }
    while (!isStopped) {
      if ( (requestQueue.isEmpty()) && (spinPolicy == SpinPolicy.SLEEP)) {
        LOG.debug("Sleeping the current thread as there are no more requests to process from the queue");
        try {
          Thread.sleep(sleepTimeMsInCaseOfNoRequests);
          continue;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      try {
        processCommand();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    LOG.info("Stop condition detected for this thread. Stopping the in-memory interpreter now...");
    stopInterpreter();
  }

  public Jep getJEP_INSTANCE()
  {
    return JEP_INSTANCE;
  }

  public void setJEP_INSTANCE(Jep JEP_INSTANCE)
  {
    this.JEP_INSTANCE = JEP_INSTANCE;
  }

  public long getTimeOutToPollFromRequestQueue()
  {
    return timeOutToPollFromRequestQueue;
  }

  public void setTimeOutToPollFromRequestQueue(long timeOutToPollFromRequestQueue)
  {
    this.timeOutToPollFromRequestQueue = timeOutToPollFromRequestQueue;
  }

  public TimeUnit getTimeUnitsToPollFromRequestQueue()
  {
    return timeUnitsToPollFromRequestQueue;
  }

  public void setTimeUnitsToPollFromRequestQueue(TimeUnit timeUnitsToPollFromRequestQueue)
  {
    this.timeUnitsToPollFromRequestQueue = timeUnitsToPollFromRequestQueue;
  }

  public boolean isStopped()
  {
    return isStopped;
  }

  public void setStopped(boolean stopped)
  {
    isStopped = stopped;
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

  public Map<PythonInterpreterConfig, Object> getInitConfigs()
  {
    return initConfigs;
  }

  public void setInitConfigs(Map<PythonInterpreterConfig, Object> initConfigs)
  {
    this.initConfigs = initConfigs;
  }

  public boolean isBusy()
  {
    boolean busyState = busyFlag;
    if (!requestQueue.isEmpty()) { // This is required because interpreter thread goes to a 1 ms sleep to allow other
      //  threads work when checking the queue for request availability. Hence busy state flag need not necessarily
      // be updated in this sleep window even though if there is a pending request
      busyState = true;
    }
    return busyState;
  }

  public void setBusy(boolean busy)
  {
    busyFlag = busy;
  }

  public SpinPolicy getSpinPolicy()
  {
    return spinPolicy;
  }

  public void setSpinPolicy(SpinPolicy spinPolicy)
  {
    this.spinPolicy = spinPolicy;
  }
}

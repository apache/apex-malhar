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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonInterpreterConfig;
import org.apache.apex.malhar.python.base.requestresponse.EvalCommandRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.GenericCommandsRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.MethodCallRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.PythonCommandType;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterResponse;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;
import org.apache.apex.malhar.python.base.requestresponse.ScriptExecutionRequestPayload;
import org.apache.apex.malhar.python.test.BasePythonTest;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

public class BaseJEPTest extends BasePythonTest
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BaseJEPTest.class);

  public static boolean JEP_INITIALIZED = false;

  private static  Object lockToInitializeJEP = new Object();

  static InterpreterThread pythonEngineThread;

  static InterpreterWrapper interpreterWrapper;

  static JepPythonEngine jepPythonEngine;

  static ExecutorService executorService = Executors.newSingleThreadExecutor();

  static BlockingQueue<PythonRequestResponse> requestQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(8, SpinPolicy.WAITING);

  static BlockingQueue<PythonRequestResponse> responseQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(8,SpinPolicy.WAITING);

  static BlockingQueue<PythonRequestResponse> delayedResponseQueueForWrapper =
      new DisruptorBlockingQueue<PythonRequestResponse>(8, SpinPolicy.WAITING);


  public static void initJEPThread() throws Exception
  {
    if (!JEP_INITIALIZED) {
      synchronized (lockToInitializeJEP) {
        if (!JEP_INITIALIZED) {
          // Interpreter for thread based tests
          pythonEngineThread = new InterpreterThread(requestQueue,responseQueue,"unittests-1");
          pythonEngineThread.preInitInterpreter(new HashMap<PythonInterpreterConfig,Object>());
          executorService.submit(pythonEngineThread);

          // interpreter wrapper for wrapper based tests
          interpreterWrapper = new InterpreterWrapper("unit-test-wrapper",delayedResponseQueueForWrapper,
              SpinPolicy.SPINNING);
          interpreterWrapper.startInterpreter();

          // JEP python engine tests
          jepPythonEngine = new JepPythonEngine("unit-tests-jeppythonengine",5);
          jepPythonEngine.preInitInterpreter(new HashMap<PythonInterpreterConfig,Object>());
          jepPythonEngine.startInterpreter();
          JEP_INITIALIZED = true;
        }
      }
    }
  }

  private void setCommonConstructsForRequestResponseObject(PythonCommandType commandType,
      PythonInterpreterRequest request, PythonRequestResponse requestResponse )
    throws ApexPythonInterpreterException
  {
    request.setCommandType(commandType);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(1L);
    requestResponse.setWindowId(1L);
    switch (commandType) {
      case EVAL_COMMAND:
        EvalCommandRequestPayload payload = new EvalCommandRequestPayload();
        request.setEvalCommandRequestPayload(payload);
        break;
      case METHOD_INVOCATION_COMMAND:
        MethodCallRequestPayload methodCallRequest = new MethodCallRequestPayload();
        request.setMethodCallRequest(methodCallRequest);
        break;
      case SCRIPT_COMMAND:
        ScriptExecutionRequestPayload scriptPayload = new ScriptExecutionRequestPayload();
        request.setScriptExecutionRequestPayload(scriptPayload);
        break;
      case GENERIC_COMMANDS:
        GenericCommandsRequestPayload payloadForGenericCommands = new GenericCommandsRequestPayload();
        request.setGenericCommandsRequestPayload(payloadForGenericCommands);
        break;
      default:
        throw new ApexPythonInterpreterException("Unsupported command type");
    }

  }

  public PythonRequestResponse<Void> buildRequestResponseObjectForVoidPayload(PythonCommandType commandType)
    throws Exception
  {
    PythonRequestResponse<Void> requestResponse = new PythonRequestResponse();
    PythonInterpreterRequest<Void> request = new PythonInterpreterRequest<>(Void.class);
    PythonInterpreterResponse<Void> response = new PythonInterpreterResponse<>(Void.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    setCommonConstructsForRequestResponseObject(commandType,request,requestResponse);
    return requestResponse;
  }

  public PythonRequestResponse<Long> buildRequestResponseObjectForLongPayload(
      PythonCommandType commandType) throws Exception
  {
    PythonRequestResponse<Long> requestResponse = new PythonRequestResponse();
    PythonInterpreterRequest<Long> request = new PythonInterpreterRequest<>(Long.class);
    requestResponse.setPythonInterpreterRequest(request);
    PythonInterpreterResponse<Long> response = new PythonInterpreterResponse<>(Long.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    setCommonConstructsForRequestResponseObject(commandType,request,requestResponse);
    return requestResponse;
  }



  public PythonRequestResponse<Integer> buildRequestResponseObjectForIntPayload(
      PythonCommandType commandType) throws Exception
  {
    PythonRequestResponse<Integer> requestResponse = new PythonRequestResponse();
    PythonInterpreterRequest<Integer> request = new PythonInterpreterRequest<>(Integer.class);
    requestResponse.setPythonInterpreterRequest(request);
    PythonInterpreterResponse<Integer> response = new PythonInterpreterResponse<>(Integer.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    setCommonConstructsForRequestResponseObject(commandType,request,requestResponse);
    return requestResponse;
  }


  protected PythonRequestResponse<Void> runCommands(List<String> commands) throws Exception
  {
    PythonRequestResponse<Void> runCommandsRequest = buildRequestResponseObjectForVoidPayload(
        PythonCommandType.GENERIC_COMMANDS);
    runCommandsRequest.getPythonInterpreterRequest().getGenericCommandsRequestPayload().setGenericCommands(commands);
    pythonEngineThread.getRequestQueue().put(runCommandsRequest);
    Thread.sleep(1000); // wait for command to be processed
    return pythonEngineThread.getResponseQueue().poll(1, TimeUnit.SECONDS);
  }


  protected PythonInterpreterRequest<Long> buildRequestObjectForLongEvalCommand(String command, String returnVar,
      Map<String,Object> paramsForEval, long timeOut, TimeUnit timeUnit, boolean deleteVariable)
  {
    PythonInterpreterRequest<Long> request = new PythonInterpreterRequest<>(Long.class);
    request.setTimeout(timeOut);
    request.setTimeUnit(timeUnit);
    EvalCommandRequestPayload evalCommandRequestPayload = new EvalCommandRequestPayload();
    request.setEvalCommandRequestPayload(evalCommandRequestPayload);
    evalCommandRequestPayload.setParamsForEvalCommand(paramsForEval);
    evalCommandRequestPayload.setDeleteVariableAfterEvalCall(deleteVariable);
    evalCommandRequestPayload.setVariableNameToExtractInEvalCall(returnVar);
    evalCommandRequestPayload.setEvalCommand(command);
    request.setExpectedReturnType(Long.class);
    return request;
  }

  protected PythonInterpreterRequest<Void> buildRequestObjectForVoidGenericCommand(List<String> commands, long timeOut,
      TimeUnit timeUnit)
  {
    PythonInterpreterRequest<Void> genericCommandRequest = new PythonInterpreterRequest<>(Void.class);
    genericCommandRequest.setTimeout(timeOut);
    genericCommandRequest.setTimeUnit(timeUnit);
    GenericCommandsRequestPayload genericCommandsRequestPayload = new GenericCommandsRequestPayload();
    genericCommandsRequestPayload.setGenericCommands(commands);
    genericCommandRequest.setExpectedReturnType(Void.class);
    genericCommandRequest.setGenericCommandsRequestPayload(genericCommandsRequestPayload);
    return genericCommandRequest;
  }


}

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
package org.apache.apex.malhar.python.base.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.apex.malhar.python.base.ApexPythonEngine;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;
import org.apache.apex.malhar.python.base.requestresponse.EvalCommandRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.GenericCommandsRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.MethodCallRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.ScriptExecutionRequestPayload;

/*** A handy utility class that implements boiler plate code while building request objects. Only commonly
 * used ones are implemented.
 *
 */
public class PythonRequestResponseUtil
{
  /***
   * Builds the request object for run commands API request. See
   * {@link ApexPythonEngine#runCommands(WorkerExecutionMode, long, long, PythonInterpreterRequest)} for details
   * @param commands
   * @param timeOut timeout for the request to complete
   * @param timeUnit Time units
   * @return A request object that can be passed to the Python Engine API for run commands
   */
  public static PythonInterpreterRequest<Void> buildRequestObjectForRunCommands(List<String> commands, long timeOut,
      TimeUnit timeUnit)
  {
    GenericCommandsRequestPayload genericCommandsRequestPayload = new GenericCommandsRequestPayload();
    genericCommandsRequestPayload.setGenericCommands(commands);
    PythonInterpreterRequest<Void> request = new PythonInterpreterRequest<>(Void.class);
    request.setTimeUnit(timeUnit);
    request.setTimeout(timeOut);
    request.setGenericCommandsRequestPayload(genericCommandsRequestPayload);
    return request;
  }

  /***
   * Builds the request object for the Eval command request. See
   * {@link ApexPythonEngine#eval(WorkerExecutionMode, long, long, PythonInterpreterRequest)} for details
   * @param evalCommand The eval expression
   * @param evalParams Variables that need to be substituted
   * @param varNameToExtract The name of variable to extract if any after the expression is evaluated. Can be null
   * @param deleteVarAfterExtract The name of the variable to delete if any. null allowed
   * @param timeOut Timeout for the API to complete processing
   * @param timeUnit Units of time for the time out variable
   * @param clazz The Class that represents the return type
   * @param <T> Template construct for Java type inference
   * @return The request object that can be used for the Eval command
   */
  public static <T> PythonInterpreterRequest<T> buildRequestForEvalCommand(
      String evalCommand, Map<String,Object> evalParams, String varNameToExtract,
      boolean deleteVarAfterExtract, long timeOut, TimeUnit timeUnit, Class<T> clazz)
  {
    PythonInterpreterRequest<T> request = new PythonInterpreterRequest<>(clazz);
    EvalCommandRequestPayload evalCommandRequestPayload = new EvalCommandRequestPayload();
    evalCommandRequestPayload.setEvalCommand(evalCommand);
    evalCommandRequestPayload.setVariableNameToExtractInEvalCall(varNameToExtract);
    evalCommandRequestPayload.setParamsForEvalCommand(evalParams);
    evalCommandRequestPayload.setDeleteVariableAfterEvalCall(deleteVarAfterExtract);
    request.setTimeUnit(timeUnit);
    request.setTimeout(timeOut);
    request.setEvalCommandRequestPayload(evalCommandRequestPayload);
    return request;
  }

  /***
   * Builds the request object for the Method call command. See
   * {@link ApexPythonEngine#executeMethodCall(WorkerExecutionMode, long, long, PythonInterpreterRequest)} for details
   * @param methodName Name of the method
   * @param methodParams parames to the method
   * @param timeOut Time allocated for completing the API
   * @param timeUnit The units of time
   * @param clazz The Class that represents the return type
   * @param <T> Java templating signature
   * @return The request object that can be used for method calls
   */
  public static <T> PythonInterpreterRequest<T> buildRequestForMethodCallCommand(
      String methodName, List<Object> methodParams, long timeOut, TimeUnit timeUnit, Class<T> clazz)
  {
    PythonInterpreterRequest<T> request = new PythonInterpreterRequest<>(clazz);
    MethodCallRequestPayload methodCallRequestPayload = new MethodCallRequestPayload();
    methodCallRequestPayload.setNameOfMethod(methodName);
    methodCallRequestPayload.setArgs(methodParams);
    request.setTimeUnit(timeUnit);
    request.setTimeout(timeOut);
    request.setMethodCallRequest(methodCallRequestPayload);
    return request;
  }

  /***
   * Builds a request object that can be used for executing the script call commands.
   * @param scriptPath Full path to the file name containing the script
   * @param timeOut The time that can be used to complete the execution of the script
   * @param timeUnit Unit of time for time out parameter
   * @param clazz The class that can be used to represent the return type
   * @param <T> Java template for type inference
   * @return The Request object that can be used for a script call invocation
   */
  public static <T> PythonInterpreterRequest<T> buildRequestForScriptCallCommand(
      String scriptPath, long timeOut, TimeUnit timeUnit, Class<T> clazz)
  {
    PythonInterpreterRequest<T> request = new PythonInterpreterRequest<>(clazz);
    ScriptExecutionRequestPayload scriptExecutionRequestPayload = new ScriptExecutionRequestPayload();
    scriptExecutionRequestPayload.setScriptName(scriptPath);
    request.setTimeUnit(timeUnit);
    request.setTimeout(timeOut);
    request.setScriptExecutionRequestPayload(scriptExecutionRequestPayload);
    return request;
  }

}

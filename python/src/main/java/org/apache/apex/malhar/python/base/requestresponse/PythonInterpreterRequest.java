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
package org.apache.apex.malhar.python.base.requestresponse;

import java.util.concurrent.TimeUnit;

public class PythonInterpreterRequest<T>
{
  PythonCommandType commandType;

  long timeout;

  TimeUnit timeUnit;

  MethodCallRequestPayload methodCallRequest;

  GenericCommandsRequestPayload genericCommandsRequestPayload;

  EvalCommandRequestPayload evalCommandRequestPayload;

  ScriptExecutionRequestPayload scriptExecutionRequestPayload;

  Class<T> expectedReturnType;

  // This constructor is not to be used by the user code and only exists for Kryo serialization
  public PythonInterpreterRequest()
  {
  }

  public PythonInterpreterRequest(Class<T> expectedReturnType)
  {
    this.expectedReturnType = expectedReturnType;
  }

  public PythonCommandType getCommandType()
  {
    return commandType;
  }

  public void setCommandType(PythonCommandType commandType)
  {
    this.commandType = commandType;
  }

  public MethodCallRequestPayload getMethodCallRequest()
  {
    return methodCallRequest;
  }

  public void setMethodCallRequest(MethodCallRequestPayload methodCallRequest)
  {
    this.methodCallRequest = methodCallRequest;
  }

  public GenericCommandsRequestPayload getGenericCommandsRequestPayload()
  {
    return genericCommandsRequestPayload;
  }

  public void setGenericCommandsRequestPayload(GenericCommandsRequestPayload genericCommandsRequestPayload)
  {
    this.genericCommandsRequestPayload = genericCommandsRequestPayload;
  }

  public EvalCommandRequestPayload getEvalCommandRequestPayload()
  {
    return evalCommandRequestPayload;
  }

  public void setEvalCommandRequestPayload(EvalCommandRequestPayload evalCommandRequestPayload)
  {
    this.evalCommandRequestPayload = evalCommandRequestPayload;
  }

  public ScriptExecutionRequestPayload getScriptExecutionRequestPayload()
  {
    return scriptExecutionRequestPayload;
  }

  public void setScriptExecutionRequestPayload(ScriptExecutionRequestPayload scriptExecutionRequestPayload)
  {
    this.scriptExecutionRequestPayload = scriptExecutionRequestPayload;
  }

  public Class<T> getExpectedReturnType()
  {
    return expectedReturnType;
  }

  public void setExpectedReturnType(Class<T> expectedReturnType)
  {
    this.expectedReturnType = expectedReturnType;
  }

  public long getTimeout()
  {
    return timeout;
  }

  public void setTimeout(long timeout)
  {
    this.timeout = timeout;
  }

  public TimeUnit getTimeUnit()
  {
    return timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit)
  {
    this.timeUnit = timeUnit;
  }
}

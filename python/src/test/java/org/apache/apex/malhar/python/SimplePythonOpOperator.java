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
package org.apache.apex.malhar.python;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.apex.malhar.python.base.ApexPythonEngine;
import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.BasePythonExecutionOperator;
import org.apache.apex.malhar.python.base.PythonInterpreterConfig;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;
import org.apache.apex.malhar.python.base.jep.SpinPolicy;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;
import org.apache.apex.malhar.python.base.util.NDimensionalArray;
import org.apache.apex.malhar.python.base.util.PythonRequestResponseUtil;

public class SimplePythonOpOperator extends BasePythonExecutionOperator<PythonProcessingPojo>
{
  private Map<String,PythonRequestResponse<NDimensionalArray>> lastKnownResponse;

  @Override
  public Map<PythonInterpreterConfig, Object> getPreInitConfigurations()
  {
    Map<PythonInterpreterConfig,Object> preInitConfigs = new HashMap<>();
    Set<String> sharedLibsList = new HashSet<>();
    sharedLibsList.add("numpy");
    preInitConfigs.put(PythonInterpreterConfig.PYTHON_SHARED_LIBS, sharedLibsList);
    // Next two configs allow for a very low latency mode wherein the cost of CPU is sacrificed for low latencies
    // Defaults are saner and the following config is overriding
    preInitConfigs.put(PythonInterpreterConfig.IDLE_INTERPRETER_SPIN_POLICY, "" + SpinPolicy.BUSY_SPIN.name());
    preInitConfigs.put(PythonInterpreterConfig.REQUEST_QUEUE_WAIT_SPIN_POLICY,
        com.conversantmedia.util.concurrent.SpinPolicy.SPINNING);
    return preInitConfigs;
  }

  @Override
  public PythonRequestResponse processPythonCodeForIncomingTuple(PythonProcessingPojo input, ApexPythonEngine pythonEngineRef)
    throws ApexPythonInterpreterException
  {
    Map<String,Object> evalParams = new HashMap<>();
    evalParams.put("intArrayToAdd",input.getNumpyIntArray());
    evalParams.put("floatArrayToAdd",input.getNumpyFloatArray());
    // Not assigning to any var as this results in output printed on the console which can be a validation of redirect
    String evalCommand = "npval=np.add(intMatrix,intArrayToAdd)";
    //String evalCommand = "print(type(intArrayToAdd))";
    PythonInterpreterRequest<NDimensionalArray> request = PythonRequestResponseUtil.buildRequestForEvalCommand(
        evalCommand,evalParams,"intMatrix",false, 20,
        TimeUnit.MILLISECONDS, NDimensionalArray.class);
    lastKnownResponse = pythonEngineRef.eval(
        WorkerExecutionMode.ANY,currentWindowId, requestCounterForThisWindow,request);
    for ( String evalCommandSubmitted: lastKnownResponse.keySet()) {
      return lastKnownResponse.get(evalCommandSubmitted); // we just need one of the N workers response.
    }
    return null;
  }

  @Override
  public void processPostSetUpPythonInstructions(ApexPythonEngine pythonEngineRef) throws ApexPythonInterpreterException
  {
    List<String> commandsToRun = new ArrayList<>();
    commandsToRun.add("import sys");
    commandsToRun.add("import numpy as np");
    commandsToRun.add("intMatrix = np.ones((2,2),dtype=int)");
    commandsToRun.add("floatMatrix = np.ones((2,2),dtype=float)");
    pythonEngineRef.runCommands(WorkerExecutionMode.BROADCAST,0L,0L,
        PythonRequestResponseUtil.buildRequestObjectForRunCommands(commandsToRun,1, TimeUnit.SECONDS));
  }

  public Map<String, PythonRequestResponse<NDimensionalArray>> getLastKnownResponse()
  {
    return lastKnownResponse;
  }

  public void setLastKnownResponse(Map<String, PythonRequestResponse<NDimensionalArray>> lastKnownResponse)
  {
    this.lastKnownResponse = lastKnownResponse;
  }
}

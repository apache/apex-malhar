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

import java.util.Map;

public class EvalCommandRequestPayload
{
  private boolean deleteVariableAfterEvalCall;

  private String variableNameToExtractInEvalCall;

  private String evalCommand;

  private Map<String,Object> paramsForEvalCommand;

  public boolean isDeleteVariableAfterEvalCall()
  {
    return deleteVariableAfterEvalCall;
  }

  public void setDeleteVariableAfterEvalCall(boolean deleteVariableAfterEvalCall)
  {
    this.deleteVariableAfterEvalCall = deleteVariableAfterEvalCall;
  }

  public String getVariableNameToExtractInEvalCall()
  {
    return variableNameToExtractInEvalCall;
  }

  public void setVariableNameToExtractInEvalCall(String variableNameToExtractInEvalCall)
  {
    this.variableNameToExtractInEvalCall = variableNameToExtractInEvalCall;
  }

  public String getEvalCommand()
  {
    return evalCommand;
  }

  public void setEvalCommand(String evalCommand)
  {
    this.evalCommand = evalCommand;
  }

  public Map<String, Object> getParamsForEvalCommand()
  {
    return paramsForEvalCommand;
  }

  public void setParamsForEvalCommand(Map<String, Object> paramsForEvalCommand)
  {
    this.paramsForEvalCommand = paramsForEvalCommand;
  }
}

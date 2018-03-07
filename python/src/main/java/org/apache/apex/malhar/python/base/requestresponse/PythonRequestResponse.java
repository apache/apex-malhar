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


public class PythonRequestResponse<T>
{
  PythonInterpreterRequest pythonInterpreterRequest;

  PythonInterpreterResponse pythonInterpreterResponse;

  long requestId;

  long windowId;

  long requestStartTime;

  long requestCompletionTime;

  public PythonInterpreterRequest getPythonInterpreterRequest()
  {
    return pythonInterpreterRequest;
  }

  public void setPythonInterpreterRequest(PythonInterpreterRequest pythonInterpreterRequest)
  {
    this.pythonInterpreterRequest = pythonInterpreterRequest;
  }

  public PythonInterpreterResponse getPythonInterpreterResponse()
  {
    return pythonInterpreterResponse;
  }

  public void setPythonInterpreterResponse(PythonInterpreterResponse pythonInterpreterResponse)
  {
    this.pythonInterpreterResponse = pythonInterpreterResponse;
  }

  public long getRequestId()
  {
    return requestId;
  }

  public void setRequestId(long requestId)
  {
    this.requestId = requestId;
  }

  public long getWindowId()
  {
    return windowId;
  }

  public void setWindowId(long windowId)
  {
    this.windowId = windowId;
  }

  public long getRequestStartTime()
  {
    return requestStartTime;
  }

  public void setRequestStartTime(long requestStartTime)
  {
    this.requestStartTime = requestStartTime;
  }

  public long getRequestCompletionTime()
  {
    return requestCompletionTime;
  }

  public void setRequestCompletionTime(long requestCompletionTime)
  {
    this.requestCompletionTime = requestCompletionTime;
  }
}


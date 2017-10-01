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
package org.apache.apex.malhar.python.operator.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.python.operator.interfaces.PythonAccumulatorWorker;
import org.apache.apex.malhar.python.operator.interfaces.PythonWorker;

import py4j.Py4JException;

public class PythonWorkerProxy<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerProxy.class);

  protected PythonWorker worker = null;
  protected PythonConstants.OpType operationType;
  protected boolean functionEnabled = false;
  protected byte[] serializedData = null;
  protected boolean workerRegistered = false;

  public PythonWorkerProxy()
  {
    this.serializedData = null;
  }

  public PythonWorkerProxy(byte[] serializedFunc)
  {
    this.serializedData = serializedFunc;
  }

  public Object execute(T tuple)
  {
    if (worker != null) {

      Object result = null;
      LOG.trace("Processing tuple: {}", tuple);
      try {
        result = worker.execute(tuple);
        LOG.trace("Processed tuple: {}", result);
        return result;
      } catch (Py4JException ex) {
        LOG.error("Exception encountered while executing operation for tuple: {}  Message: {}", tuple, ex.getMessage());
      }
    }
    return null;
  }

  public void register(PythonWorker pythonWorker)
  {
    if (pythonWorker == null) {
      throw new RuntimeException("Null Python Worker");
    }
    LOG.debug("Current proxy instance {}", this);
    LOG.debug("Registering python worker now {} {}", (pythonWorker != null), (pythonWorker instanceof PythonAccumulatorWorker));
    this.worker = pythonWorker;
    this.workerRegistered = true;
    LOG.debug("Python worker registered");
  }

  public void setSerializedData(String opType)
  {
    if (this.isWorkerRegistered() && !isFunctionEnabled()) {
      LOG.debug("Setting Serialized function");
      this.worker.setFunction(this.serializedData, opType);
      this.functionEnabled = true;
      LOG.debug("Set Serialized function");
    }
  }

  public byte[] getSerializedData()
  {
    return serializedData;
  }

  public PythonWorker getWorker()
  {
    return worker;
  }

  public PythonConstants.OpType getOperationType()
  {
    return operationType;
  }

  public void setOperationType(PythonConstants.OpType operationType)
  {
    this.operationType = operationType;
  }

  public boolean isWorkerRegistered()
  {
    return this.workerRegistered;
  }

  public boolean isFunctionEnabled()
  {
    return this.functionEnabled;
  }
}

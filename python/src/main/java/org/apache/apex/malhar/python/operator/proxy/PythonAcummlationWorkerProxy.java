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

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.python.operator.interfaces.PythonAccumulatorWorker;

import py4j.Py4JException;

public class PythonAcummlationWorkerProxy<T> extends PythonWorkerProxy<T> implements Accumulation<T, T, T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonAcummlationWorkerProxy.class);

  public PythonAcummlationWorkerProxy()
  {
    super();

  }

  public PythonAcummlationWorkerProxy(byte[] serializedFunc)
  {
    super(serializedFunc);
    this.serializedData = serializedFunc;
  }

  @Override
  public T defaultAccumulatedValue()
  {
    return null;
  }

  @Override
  public T accumulate(T accumulatedValue, T input)
  {
    if (getWorker() != null) {

      T result = null;
      LOG.trace("Processing accumulation: {}", input);
      try {
        result = (T)((PythonAccumulatorWorker)getWorker()).accumulate(accumulatedValue, input);
        LOG.trace("Processed accumulation: {}", result);
        return result;
      } catch (Py4JException ex) {
        LOG.error("Exception encountered while executing operation for tuple: {}  Message: {}", input, ex.getMessage());
      } finally {
        return null;
      }
    }
    return null;

  }

  @Override
  public T merge(T accumulatedValue1, T accumulatedValue2)
  {
    if (getWorker() != null) {

      T result = null;
      LOG.trace("Processing accumulation: {} {}", accumulatedValue1, accumulatedValue2);
      try {
        result = (T)((PythonAccumulatorWorker)getWorker()).merge(accumulatedValue1, accumulatedValue2);
        LOG.trace("Processed accumulation: {}", result);
        return result;
      } catch (Py4JException ex) {
        LOG.error("Exception encountered while executing operation for accumulation: {} {}  Message: {}", accumulatedValue1, accumulatedValue2, ex.getMessage());
      } finally {
        return null;
      }
    }
    return null;

  }

  @Override
  public T getOutput(T accumulatedValue)
  {
    return null;
  }

  @Override
  public T getRetraction(T value)
  {
    return null;
  }
}

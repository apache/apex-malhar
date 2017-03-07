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
import org.apache.apex.malhar.lib.window.accumulation.Reduce;
import org.apache.apex.malhar.python.operator.interfaces.PythonReduceWorker;

public class PythonReduceProxy<T> extends PythonAcummlationWorkerProxy<T> implements Reduce<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonReduceProxy.class);
  private Class<T> clazz;

  public PythonReduceProxy()
  {
    super();
  }

  public PythonReduceProxy(PythonConstants.OpType operationType, byte[] serializedFunc, Class<T> clazz)
  {
    super(serializedFunc);
    this.operationType = operationType;
    this.clazz = clazz;
  }

  @Override
  public T defaultAccumulatedValue()
  {
    T generatedDefaultValue = getInstance();
    LOG.debug("defaultAccumulatedValue received {} " + generatedDefaultValue);
    return generatedDefaultValue;
  }

  @Override
  public T getOutput(T accumulatedValue)
  {
    LOG.debug("getOutput received {}", accumulatedValue);
    if (accumulatedValue == null) {
      accumulatedValue = getInstance();
    }
    return accumulatedValue;
  }

  @Override
  public T getRetraction(T value)
  {
    LOG.debug("Retraction received {}", value);
    return null;
  }

  @Override
  public T accumulate(T accumulatedValue, T input)
  {
    LOG.debug("Accumulate call received {}", input);

    if (accumulatedValue == null) {
      return input;
    }
    return reduce(accumulatedValue, input);
  }

  @Override
  public T merge(T accumulatedValue1, T accumulatedValue2)
  {
    LOG.debug("Merge received {} {} ", accumulatedValue1, accumulatedValue1);
    return reduce(accumulatedValue1, accumulatedValue2);
  }

  @Override
  public T reduce(T input1, T input2)
  {
    LOG.debug("Reduce Input received {} {} ", input1, input2);

    T result = (T)((PythonReduceWorker)getWorker()).reduce(input1, input2);
    LOG.debug("Reduce Output generated {}", result);
    return result;
  }

  protected T getInstance()
  {
    try {
      return clazz.newInstance();
    } catch (InstantiationException e) {
      LOG.error("Failed to instantiate class {} " + clazz.getName());
    } catch (IllegalAccessException e) {
      LOG.error("Failed to instantiate class {} " + clazz.getName());
    }
    return null;
  }
}

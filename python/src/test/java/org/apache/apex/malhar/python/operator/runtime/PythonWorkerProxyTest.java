/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.operator.runtime;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.operator.interfaces.PythonWorker;
import org.apache.apex.malhar.python.operator.proxy.PythonWorkerProxy;

import py4j.Py4JException;

import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PythonWorkerProxy.class, LoggerFactory.class})
public class PythonWorkerProxyTest
{

  public static class PythonTestWorker implements PythonWorker
  {

    @Override
    public Object setFunction(byte[] func, String opType)
    {
      return opType;
    }

    @Override
    public Object execute(Object tuple)
    {
      return tuple;
    }

  }

  @Test
  public void testPythonWorkerRegisterAndExecute()
  {
    mockStatic(LoggerFactory.class);
    Logger logger = mock(Logger.class);
    when(LoggerFactory.getLogger(PythonWorkerProxy.class)).thenReturn(logger);

    String functionData = new String("TestFunction");
    PythonWorkerProxy workerProxy = new PythonWorkerProxy(functionData.getBytes());
    PythonTestWorker worker = new PythonTestWorker();
    workerProxy.register(worker);
    workerProxy.setSerializedData("DUMMY_OPERATION");
    Assert.assertEquals("TUPLE", worker.execute("TUPLE"));
  }

  @Test()
  public void testPythonFailureWhileProcessingTuple()
  {
    mockStatic(LoggerFactory.class);
    Logger logger = mock(Logger.class);
    when(LoggerFactory.getLogger(any(Class.class))).thenReturn(logger);

    String exceptionString = "DUMMY EXCEPTION";
    String functionData = new String("TestFunction");
    PythonWorker failingMockWorker = mock(PythonWorker.class);
    when(failingMockWorker.execute("TUPLE")).thenThrow(new Py4JException(exceptionString));

    PythonWorkerProxy workerProxy = new PythonWorkerProxy(functionData.getBytes());
    workerProxy.register(failingMockWorker);
//    verify(logger).debug("Registering python worker now");
//    verify(logger).debug("Python worker registered");
    String tupleValue = "TUPLE";
    Assert.assertEquals(null, workerProxy.execute(tupleValue));

//    verify(logger).trace("Processing tuple:" + tupleValue);
//    verify(logger).error("Exception encountered while executing operation for tuple:" + tupleValue + " Message:" + exceptionString);

  }
}

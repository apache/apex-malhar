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
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.python.test.JepPythonTestContext;
import org.apache.apex.malhar.python.test.PythonAvailabilityTestRule;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;

import static org.junit.Assert.assertEquals;

public class PythonExecutorApplicationTest
{
  private static final List<Object> results = Collections.synchronizedList(new ArrayList<>());

  private static final transient Logger LOG = LoggerFactory.getLogger(PythonExecutorApplicationTest.class);

  @Rule
  public PythonAvailabilityTestRule jepAvailabilityBasedTest = new PythonAvailabilityTestRule();


  @SuppressWarnings("serial")
  private static final Function.MapFunction<Object, Void> outputFn = new Function.MapFunction<Object, Void>()
  {
    @Override
    public Void f(Object input)
    {
      results.add(input);
      return null;
    }
  };

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testApplication() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(Launcher.LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true);
    PythonExecutorApplication pythonExecutorApplication = new PythonExecutorApplication();
    pythonExecutorApplication.outputFn =  outputFn;
    Launcher.AppHandle appHandle = launcher.launchApp(pythonExecutorApplication, conf, launchAttributes);
    int sleepTimeCounterForLoopExit = 0;
    int sleepTimePerIteration = 1000;
    // wait until expected result count or timeout
    while (results.size() < pythonExecutorApplication.pojoDataGenerator.getMaxTuples()) {
      sleepTimeCounterForLoopExit += sleepTimePerIteration;
      if (sleepTimeCounterForLoopExit > 30000) {
        break;
      }
      LOG.info("Test sleeping until the application time out is reached");
      Thread.sleep(sleepTimePerIteration);
    }
    appHandle.shutdown(Launcher.ShutdownMode.KILL);
    assertEquals(pythonExecutorApplication.pojoDataGenerator.getMaxTuples(), results.size());
  }

}








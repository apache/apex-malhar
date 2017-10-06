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

package org.apache.apex.examples.transform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class SimpleTransformApplicationTest
{
  private static final List<Object> results = Collections.synchronizedList(new ArrayList<>());

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

  @Test
  public void testApplication() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(Launcher.LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true);
    SimpleTransformApplication simpleTransformApplication = new SimpleTransformApplication();
    simpleTransformApplication.outputFn = outputFn;
    Launcher.AppHandle appHandle = launcher.launchApp(simpleTransformApplication, conf, launchAttributes);
    int sleepTimeCounterForLoopExit = 0;
    int sleepTimePerIteration = 500;
    // wait until expected result count or timeout
    while (results.size() < simpleTransformApplication.pojoDataGenerator.getMaxTuples()) {
      sleepTimeCounterForLoopExit += sleepTimePerIteration;
      if (sleepTimeCounterForLoopExit > 30000) {
        break;
      }
      Thread.sleep(sleepTimePerIteration);
    }
    appHandle.shutdown(Launcher.ShutdownMode.KILL);
    assertEquals(results.size(), simpleTransformApplication.pojoDataGenerator.getMaxTuples());
    assertTransformationsGenerated(simpleTransformApplication);
  }

  private void assertTransformationsGenerated(SimpleTransformApplication simpleTransformApplication) throws IOException
  {
    int maxLengthOfCustomerName = 1 + ( 2 * simpleTransformApplication.pojoDataGenerator.getMaxNameLength()); //first name + last name + space
    for (Object customerInfoObject : results) {
      CustomerInfo aTransformedCustomer = (CustomerInfo)customerInfoObject;
      assertFalse(aTransformedCustomer.getAddress().matches("[A-Z]"));
      assertThat(aTransformedCustomer.getAge(), Matchers.lessThanOrEqualTo(POJOGenerator.MAX_YEAR_DIFF_RANGE));
      assertThat(aTransformedCustomer.getName().length(), Matchers.lessThanOrEqualTo(maxLengthOfCustomerName));
    }
  }
}

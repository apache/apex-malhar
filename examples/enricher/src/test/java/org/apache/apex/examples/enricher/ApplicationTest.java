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
package org.apache.apex.examples.enricher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.ShutdownMode;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
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
    EnricherAppWithJSONFile enricherAppWithJSONFile = new EnricherAppWithJSONFile();
    enricherAppWithJSONFile.outputFn = outputFn;
    Launcher.AppHandle appHandle = launcher.launchApp(enricherAppWithJSONFile, conf, launchAttributes);
    int sleepTimeCounterForLoopExit = 0;
    int sleepTimePerIteration = 500;
    // wait until expected result count or timeout
    while (results.size() < enricherAppWithJSONFile.getDataGenerator().getLimit()) {
      sleepTimeCounterForLoopExit += sleepTimePerIteration;
      if (sleepTimeCounterForLoopExit > 30000) {
        break;
      }
      Thread.sleep(sleepTimePerIteration);
    }
    appHandle.shutdown(ShutdownMode.KILL);
    assertTrue(results.size() >= enricherAppWithJSONFile.getDataGenerator().getLimit());
    assertEnrichedOutput();
  }

  private void assertEnrichedOutput() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    Map<String,Integer> countsOfEnrichedValues = new HashMap<>();
    String enrichedValue = null;
    for (Object result : results) {
      POJOEnriched anEnrichedEntry = mapper.readValue(result.toString(), POJOEnriched.class);
      enrichedValue = anEnrichedEntry.getCircleName();
      assertNotNull(enrichedValue); // all values should have an entiched value
      if (!countsOfEnrichedValues.containsKey(enrichedValue)) {
        countsOfEnrichedValues.put(enrichedValue,1);
      } else {
        Integer currentCountForEnrichedEntry = countsOfEnrichedValues.get(enrichedValue);
        countsOfEnrichedValues.put(enrichedValue, currentCountForEnrichedEntry + 1);
      }
    }
    for (String circleName : countsOfEnrichedValues.keySet() ) {
      int currentCircleNameAsciiCode = (circleName.charAt(0));
      Integer circleNameCount = countsOfEnrichedValues.get(circleName);
      // All circle names  "A" to "E" should have a count of 2 and all others should have 1
      if ( ( currentCircleNameAsciiCode > 64 ) && (currentCircleNameAsciiCode < 70) ) {
        assertEquals(2,circleNameCount.intValue());
      } else {
        assertEquals(1, circleNameCount.intValue());
      }
    }
  }
}

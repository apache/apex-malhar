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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{

  @Test
  public void testApplication() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(Launcher.LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true);
    EnricherAppWithJSONFile enricherAppWithJSONFile = new EnricherAppWithJSONFile();
    Launcher.AppHandle appHandle = launcher.launchApp(enricherAppWithJSONFile, conf, launchAttributes);
    File folder = new File("target/enrichedData");
    File[] listOfFiles = null;
    int sleepTimeCounterForLoopExit = 0;
    int sleepTimePerIteration = 5000;
    // First ensure that the output file is generated and the expected lines match
    // The test main thread sleeps until the expected lines are generated in the output file
    while (true) {
      sleepTimeCounterForLoopExit += sleepTimePerIteration;
      // break the loop of the test output is not generated by 40 seconds
      if (sleepTimeCounterForLoopExit > 40000) {
        break;
      }
      Thread.sleep(sleepTimePerIteration);
      listOfFiles = folder.listFiles();
      if ( listOfFiles.length == 0) {
        continue;
      }
      String lineRead = null;
      int counter = 0;
      try (BufferedReader reader = new BufferedReader(new FileReader(listOfFiles[0].getAbsolutePath()))) {
        while ((lineRead = reader.readLine()) != null) {
          counter += 1;
        }
      }
      if (counter < enricherAppWithJSONFile.getDataGenerator().getLimit()) {
        continue;
      } else {
        break;
      }
    }
    listOfFiles = folder.listFiles();
    assertEnrichedOutput(listOfFiles);
  }

  private void assertEnrichedOutput(File[] listOfFiles) throws IOException
  {
    // Ensure that only one file is generated as output per properties configuration
    assertEquals(1, listOfFiles.length);
    String aJSONEntry = null;
    ObjectMapper mapper = new ObjectMapper();
    Map<String,Integer> countsOfEnrichedValues = new HashMap<>();
    String enrichedValue = null;
    try (BufferedReader reader = new BufferedReader(new FileReader(listOfFiles[0].getAbsolutePath()))) {
      while ((aJSONEntry = reader.readLine()) != null) {
        POJOEnriched anEnrichedEntry = mapper.readValue(aJSONEntry, POJOEnriched.class);
        enrichedValue = anEnrichedEntry.getCircleName();
        assertNotNull(enrichedValue); // all values should have an entiched value
        if (!countsOfEnrichedValues.containsKey(enrichedValue)) {
          countsOfEnrichedValues.put(enrichedValue,1);
        } else {
          Integer currentCountForEnrichedEntry = countsOfEnrichedValues.get(enrichedValue);
          countsOfEnrichedValues.put(enrichedValue, currentCountForEnrichedEntry + 1);
        }
      }
    }
    for (String circleName : countsOfEnrichedValues.keySet() ) {
      int currentCircleNameAsciiCode = ((int)circleName.charAt(0));
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

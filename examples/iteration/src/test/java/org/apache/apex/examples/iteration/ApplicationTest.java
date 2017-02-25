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
package org.apache.apex.examples.iteration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 *
 */
public class ApplicationTest
{
  @Test
  public void testIterationApp() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    Application app = new Application();
    String outputFileName = "target/output.txt";
    long timeout = 10 * 1000; // 10 seconds

    new File(outputFileName).delete();
    app.setExtraOutputFileName(outputFileName);
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    long startTime = System.currentTimeMillis();
    do {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {
        break;
      }
      File file = new File(outputFileName);
      if (file.length() > 50) {
        break;
      }
    }
    while (System.currentTimeMillis() - startTime < timeout);

    lc.shutdown();
    try (BufferedReader br = new BufferedReader(new FileReader(outputFileName))) {
      Assert.assertEquals("1", br.readLine());
      Assert.assertEquals("1", br.readLine());
      Assert.assertEquals("2", br.readLine());
      Assert.assertEquals("3", br.readLine());
      Assert.assertEquals("5", br.readLine());
      Assert.assertEquals("8", br.readLine());
      Assert.assertEquals("13", br.readLine());
      Assert.assertEquals("21", br.readLine());
      Assert.assertEquals("34", br.readLine());
      Assert.assertEquals("55", br.readLine());
      Assert.assertEquals("89", br.readLine());
      Assert.assertEquals("144", br.readLine());
      Assert.assertEquals("233", br.readLine());
      Assert.assertEquals("377", br.readLine());
      Assert.assertEquals("610", br.readLine());
      Assert.assertEquals("987", br.readLine());
    }
  }
}

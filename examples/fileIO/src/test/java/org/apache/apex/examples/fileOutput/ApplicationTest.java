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

package org.apache.apex.examples.fileOutput;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import javax.validation.ConstraintViolationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;


/**
 * Test application in local mode.
 */
public class ApplicationTest
{

  private static final String baseDirName   = "target/fileOutput";
  private static final String outputDirName = baseDirName + "/output-dir";
  private static final File outputDirFile   = new File(outputDirName);

  private void cleanup()
  {
    try {
      FileUtils.deleteDirectory(outputDirFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // check that the requisite number of files exist in the output directory
  private boolean check()
  {
    // Look for files with a single digit extension
    String[] ext = {"0","1","2","3","4","5","6","7","8","9"};
    Collection<File> list = FileUtils.listFiles(outputDirFile, ext, false);

    return !list.isEmpty();
  }

  // return Configuration with suitable properties set
  private Configuration getConfig()
  {
    final Configuration result = new Configuration(false);
    result.addResource(this.getClass().getResourceAsStream("/META-INF/properties-fileOutput.xml"));
    result.setInt("dt.application.fileOutput.dt.operator.generator.prop.divisor", 3);
    result.set("dt.application.fileOutput.operator.writer.prop.filePath", outputDirName);
    return result;
  }

  @Before
  public void beforeTest() throws Exception
  {
    cleanup();
    FileUtils.forceMkdir(outputDirFile);
  }

  @After
  public void afterTest()
  {
    cleanup();
  }

  @Test
  public void testApplication() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      lma.prepareDAG(new Application(), getConfig());
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      // wait for output files to show up
      while (!check()) {
        Thread.sleep(1000);
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}

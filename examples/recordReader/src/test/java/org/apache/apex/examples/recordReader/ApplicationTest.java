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

package org.apache.apex.examples.recordReader;

import java.io.File;
import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Test application in local mode.
 */
public class ApplicationTest
{
  private String outputDir;

  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      try {
        FileUtils.forceDelete(new File(baseDirectory));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Before
  public void setup() throws Exception
  {
    outputDir = testMeta.baseDirectory + File.separator + "output";
  }

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      conf.set("dt.application.RecordReaderExample.operator.fileOutput.prop.filePath", outputDir);
      File outputfile = FileUtils.getFile(outputDir, "output.txt_5.0");

      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      // wait for tuples to show up
      while (!outputfile.exists()) {
        Thread.sleep(1000);
      }

      lc.shutdown();
      Assert.assertTrue(FileUtils.contentEquals(
          FileUtils.getFile(
          conf.get("dt.application.RecordReaderExample.operator.recordReader.prop.files")),
          outputfile));

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}

/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.tutorial.filter;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

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
import com.datatorrent.stram.StramLocalCluster;

/**
 * Test the DAG declaration in local mode.
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
      conf.set("dt.application.FilterExample.operator.selectedOutput.prop.filePath", outputDir);
      conf.set("dt.application.FilterExample.operator.rejectedOutput.prop.filePath", outputDir);
      final File selectedfile = FileUtils.getFile(outputDir, "selected.txt_8.0");
      final File rejectedfile = FileUtils.getFile(outputDir, "rejected.txt_6.0");
      
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();

      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          if (selectedfile.exists() && rejectedfile.exists()) {
            return true;
          }
          return false;
        }
      });
      
      lc.run(40000);
      Assert.assertTrue(
          FileUtils.contentEquals(
              FileUtils.getFile(
                  "src/main/resources/META-INF/selected_output.txt"
                  ),selectedfile));
      
      Assert.assertTrue(
          FileUtils.contentEquals(
              FileUtils.getFile(
                  "src/main/resources/META-INF/rejected_output.txt"
                  ),rejectedfile));

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}

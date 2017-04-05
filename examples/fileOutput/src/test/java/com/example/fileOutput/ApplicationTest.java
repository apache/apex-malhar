package com.example.fileOutput;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import javax.validation.ConstraintViolationException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.api.LocalMode;


/**
 * Test application in local mode.
 */
public class ApplicationTest {

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

    return ! list.isEmpty();
  }

  // return Configuration with suitable properties set
  private Configuration getConfig()
  {
    final Configuration result = new Configuration(false);
    result.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    result.setInt("dt.application.fileOutput.dt.operator.generator.prop.divisor", 3);
    result.set("dt.application.fileOutput.operator.writer.prop.filePath", outputDirName);
    return result;
  }

  @Before
  public void beforeTest() throws Exception {
    cleanup();
    FileUtils.forceMkdir(outputDirFile);
  }

  @After
  public void afterTest() {
    cleanup();
  }

  @Test
  public void testApplication() throws Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      lma.prepareDAG(new Application(), getConfig());
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      // wait for output files to show up
      while ( ! check() ) {
        System.out.println("Sleeping ....");
        Thread.sleep(1000);
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}

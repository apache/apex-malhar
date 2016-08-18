package com.demo.myapexapp;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import javax.validation.ConstraintViolationException;

import org.apache.commons.io.FileUtils;

import org.junit.AfterClass;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;

import org.junit.Test;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{

  private static final String FILE_NAME = "/tmp/formatterApp";

  @AfterClass
  public static void cleanup()
  {
    try {
      FileUtils.deleteDirectory(new File(FILE_NAME));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testApplication() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      // wait for output files to roll
      Thread.sleep(5000);

      String[] extensions = {"dat.0", "tmp"};
      Collection<File> list = FileUtils.listFiles(new File(FILE_NAME), extensions, false);

      for (File file : list) {
        for (String line : FileUtils.readLines(file)) {
          Assert.assertEquals("Delimiter in record", true, (line.equals(
            "1234|0|SimpleCsvFormatterExample|10000.0|||APEX|false|false||")));
        }
      }

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}

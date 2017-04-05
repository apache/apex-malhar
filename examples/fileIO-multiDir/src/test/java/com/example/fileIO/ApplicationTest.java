package com.example.fileIO;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import javax.validation.ConstraintViolationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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

  private static final String baseDirName   = "target/fileIO";
  private static final String inputDirName1 = baseDirName + "/in1";
  private static final String inputDirName2 = baseDirName + "/in2";
  private static final String outputDirName = baseDirName + "/output-dir";
  private static final File inputDirFile1   = new File(inputDirName1);
  private static final File inputDirFile2   = new File(inputDirName2);
  private static final File outputDirFile   = new File(outputDirName);

  private static final int numFiles      = 10;    // number of input files
  private static final int numLines      = 10;    // number of lines in each input file
  private static final int numPartitions = 3;     // number of partitions of input operator

  // create nFiles files with nLines lines in each
  private void createFiles() throws IOException {
    
    String[] lines1 = {"line-1", "line-2"}, lines2 = {"line-3", "line-4"};
    try {
      FileUtils.write(new File(inputDirFile1, "file1.txt"),
                      StringUtils.join(lines1, "\n"));
      FileUtils.write(new File(inputDirFile2, "file2.txt"),
                      StringUtils.join(lines2, "\n"));
    } catch (IOException e) {
      System.out.format("Error: Failed to create file %s%n");
      e.printStackTrace();
    }
    System.out.format("Created 2 files with 2 lines in each%n");
  }

  private void cleanup()
  {
    try {
      FileUtils.deleteDirectory(inputDirFile1);
      FileUtils.deleteDirectory(inputDirFile2);
      FileUtils.deleteDirectory(outputDirFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // check that the requisite number of files exist in the output directory
  private boolean check(final int nFiles)
  {
    String[] ext = {"txt"};
    Collection<File> list = FileUtils.listFiles(outputDirFile, ext, false);
    
    if (list.size() < 2)
      return false;

    // we have 2 files; check that the rename from .tmp to .txt has happened
    int found = 0;
    for (File f : list) {
      String name = f.getName();
      if (name.equals("file1.txt") || name.equals("file2.txt")) {
        ++found;
      }
    }
    return 2 == found;
  }

  // return Configuration with suitable properties set
  private Configuration getConfig()
  {
    final Configuration result = new Configuration(false);
    //conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    result.set("dt.application.FileIO.operator.read.prop.directory", "dummy");
    result.set("dt.application.FileIO.operator.read.prop.directories",
               "\"target/fileIO/in1\",\"target/fileIO/in2\"");
    result.set("dt.application.FileIO.operator.read.prop.partitionCounts", "2,3");
    result.set("dt.application.FileIO.operator.write.prop.filePath", outputDirName);
    return result;
  }

  @Before
  public void beforeTest() throws Exception {
    cleanup();
    FileUtils.forceMkdir(inputDirFile1);
    FileUtils.forceMkdir(inputDirFile2);
    FileUtils.forceMkdir(outputDirFile);

    // create some text files in input directory
    createFiles();
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
      while ( ! check(numFiles) ) {
        System.out.println("Sleeping ....");
        Thread.sleep(1000);
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}

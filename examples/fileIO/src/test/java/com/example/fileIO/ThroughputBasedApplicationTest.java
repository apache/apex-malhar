package com.example.fileIO;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.validation.ConstraintViolationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

public class ThroughputBasedApplicationTest
{
  private static final String baseDirName = "target/fileIO";
  private static final String inputDirName = baseDirName + "input-dir";
  private static final String outputDirName = baseDirName + "output-dir";
  private static final File inputDirFile = new File(inputDirName);
  private static final File outputDirFile = new File(outputDirName);

  private static final int numFiles = 10; // number of input files
  private static final int numLines = 10; // number of lines in each input file
  private static final int numPartitions = 4; // number of partitions of input operator

  // create nFiles files with nLines lines in each
  private void createFiles(final int nFiles, final int nLines) throws IOException
  {
    for (int file = 0; file < nFiles; file++) {
      ArrayList<String> lines = new ArrayList<>();
      for (int line = 0; line < nLines; line++) {
        lines.add("file " + file + ", line " + line);
      }
      try {
        FileUtils.write(new File(inputDirFile, "file" + file), StringUtils.join(lines, "\n"));
      } catch (IOException e) {
        System.out.format("Error: Failed to create file %s%n", file);
        e.printStackTrace();
      }
    }
    System.out.format("Created %d files with %d lines in each%n", nFiles, nLines);
  }

  private void cleanup()
  {
    try {
      FileUtils.deleteDirectory(inputDirFile);
      FileUtils.deleteDirectory(outputDirFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // check that the requisite number of files exist in the output directory
  private boolean checkFilesCopied(final int nFiles)
  {
    return (nFiles == FileUtils.listFiles(outputDirFile, null, false).size() && verifyFilesRenamed());
  }

  private boolean verifyFilesRenamed()
  {
    for (int i = 0; i < numFiles; i++) {
      String fileName = "file" + i;
      if (!new File(outputDirFile, fileName).exists()) {
        return false;
      }
    }
    return true;
  }

  private Configuration getConfig()
  {
    final Configuration result = new Configuration(false);
    result.setInt("dt.application.ThroughputBasedFileIO.attr.CHECKPOINT_WINDOW_COUNT", 10);
    result.set("dt.application.ThroughputBasedFileIO.operator.read.prop.directory", inputDirName);
    result.setInt("dt.application.ThroughputBasedFileIO.operator.read.prop.pendingFilesPerOperator", 2);
    result.setInt("dt.application.ThroughputBasedFileIO.operator.read.prop.partitionCount", numPartitions);
    result.set("dt.application.ThroughputBasedFileIO.operator.write.prop.filePath", outputDirName);
    return result;
  }

  @Before
  public void beforeTest() throws Exception
  {
    cleanup();
    FileUtils.forceMkdir(inputDirFile);
    FileUtils.forceMkdir(outputDirFile);

    // create some text files in input directory
    createFiles(numFiles, numLines);
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
      lma.prepareDAG(new ThroughputBasedApplication(), getConfig());
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      // wait for output files to show up
      while (!checkFilesCopied(numFiles)) {
        System.out.println("Waiting for files to get copied.");
        Thread.sleep(1000);
      }
      lc.shutdown();
      verifyCopiedData();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private void verifyCopiedData() throws IOException
  {
    for (int i = 0; i < numFiles; i++) {
      String fileName = "file" + i;
      Assert.assertTrue(FileUtils.contentEquals(new File(inputDirFile, fileName), new File(outputDirName, fileName)));
    }
  }
}

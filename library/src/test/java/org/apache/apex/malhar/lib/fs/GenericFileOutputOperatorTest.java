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

package org.apache.apex.malhar.lib.fs;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.BytesFileOutputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.commons.io.FileUtils;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperatorTest;
import com.datatorrent.netlet.util.DTThrowable;

public class GenericFileOutputOperatorTest extends AbstractFileOutputOperatorTest
{

  /**
   * Test file rollover in case of idle windows
   * 
   * @throws IOException
   */
  @Test
  public void testIdleWindowsFinalize() throws IOException
  {
    StringFileOutputOperator writer = new StringFileOutputOperator();
    writer.setOutputFileName("output.txt");
    writer.setFilePath(testMeta.getDir());
    writer.setAlwaysWriteToTmp(true);
    writer.setMaxIdleWindows(5);
    writer.setup(testMeta.testOperatorContext);

    String[][] tuples = {{"0a", "0b" }, {"1a", "1b" }, {}, {}, {}, {}, {"6a", "6b" }, {"7a", "7b" }, {}, {}, {},
        {}, {}, {"13a", "13b" }, {"14a", "14b" }, {}, {}, {}, {"18a", "18b" }, {"19a", "19b" }, {}, {}, {}, {}, {},
        {}, {"26a", "26b"} };

    for (int i = 0; i <= 12; i++) {
      writer.beginWindow(i);
      for (String t : tuples[i]) {
        writer.input.put(t);
      }
      writer.endWindow();
    }
    writer.committed(10);

    for (int i = 13; i <= 26; i++) {
      writer.beginWindow(i);
      for (String t : tuples[i]) {
        writer.input.put(t);
      }
      writer.endWindow();
    }
    writer.committed(20);
    writer.committed(26);

    String[] expected = {"0a\n0b\n1a\n1b\n6a\n6b\n7a\n7b\n", "13a\n13b\n14a\n14b\n18a\n18b\n19a\n19b\n",
        "26a\n26b\n" };

    for (int i = 0; i < expected.length; i++) {
      checkOutput(i, testMeta.getDir() + "/output.txt_0", expected[i], true);
    }
  }

  /**
   * Test file rollover for tuple count
   * 
   * @throws IOException
   */
  @Test
  public void testTupleCountFinalize() throws IOException
  {
    BytesFileOutputOperator writer = new BytesFileOutputOperator();
    writer.setOutputFileName("output.txt");
    writer.setFilePath(testMeta.getDir());
    writer.setAlwaysWriteToTmp(true);
    writer.setMaxTupleCount(10);
    writer.setup(testMeta.testOperatorContext);

    String[][] tuples = {{"0a", "0b" }, {"1a", "1b" }, {}, {"3a", "3b" }, {"4a", "4b" }, {}, {"6a", "6b" },
        {"7a", "7b" }, {}, {}, {"9a" }, {"10a", "10b" }, {}, {"12a" }, {"13a", "13b"}, {"14a", "14b" }, {}, {},
        {}, {"18a", "18b" }, {"19a", "19b" }, {"20a" }, {"21a" }, {"22a" }};

    for (int i = 0; i < tuples.length; i++) {
      writer.beginWindow(i);
      for (String t : tuples[i]) {
        writer.input.put(t.getBytes());
      }
      writer.endWindow();
      if (i % 10 == 0) {
        writer.committed(10);
      }
    }
    writer.committed(tuples.length);

    String[] expected = {"0a\n0b\n1a\n1b\n3a\n3b\n4a\n4b\n6a\n6b\n", "7a\n7b\n9a\n10a\n10b\n12a\n13a\n13b\n14a\n14b\n",
        "18a\n18b\n19a\n19b\n20a\n21a\n22a\n" };

    for (int i = 0; i < expected.length; i++) {
      checkOutput(i, testMeta.getDir() + "/output.txt_0", expected[i], true);
    }
  }
  
  public static void checkOutput(int fileCount, String baseFilePath, String expectedOutput, boolean checkTmp)
  {
    if (fileCount >= 0) {
      baseFilePath += "." + fileCount;
    }

    File file = new File(baseFilePath);

    if (!file.exists()) {
      String[] extensions = {"tmp"};
      Collection<File> tmpFiles = FileUtils.listFiles(file.getParentFile(), extensions, false);
      for (File tmpFile : tmpFiles) {
        if (file.getPath().startsWith(baseFilePath)) {
          file = tmpFile;
          break;
        }
      }
    }

    String fileContents = null;

    try {
      fileContents = FileUtils.readFileToString(file);
    } catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }

    Assert.assertEquals("Single file " + fileCount + " output contents", expectedOutput, fileContents);
  }
}

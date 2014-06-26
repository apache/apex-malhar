/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.*;
import java.util.Calendar;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.fs.HdfsFileDataOutputOperator.FileData;

/**
 * Functional test for {@link HdfsFileDataOutputOperator}
 */
public class HdfsFileDataOutputOperatorTest
{
  @Test
  public void testFileOutputOperator()
  {
    HdfsFileDataOutputOperator oper = new HdfsFileDataOutputOperator();
    oper.setFilePath("target");
    oper.setAppend(false);
    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0));
    oper.beginWindow(0);
    FileData fileData1 = new FileData();
    fileData1.info.name = "file1";
    fileData1.data = "data1";

    oper.input.process(fileData1);

    FileData fileData2 = new FileData();
    fileData2.info.name = "file2";
    fileData2.data = "data2";
    oper.input.process(fileData2);

    FileData fileData3 = new FileData();
    fileData3.info.name = "file1";
    fileData3.data = "data2";

    oper.input.process(fileData3);

    oper.endWindow();
    oper.teardown();
    Assert.assertEquals("The number of lines in file target/file-0-0", 1, readFile("target/file1", "data2"));
    Assert.assertEquals("The number of lines in file target/file-0-1", 1, readFile("target/file2", "data2"));
    Assert.assertEquals("Checking the file target/file-0-0", true, checkFile("target/file1"));
    Assert.assertEquals("Checking the file target/file-0-1", true, checkFile("target/file2"));

  }

  private int readFile(String path, final String val)
  {
    BufferedReader br = null;
    try {
      FileInputStream fstream = new FileInputStream(path);
      DataInputStream in = new DataInputStream(fstream);
      br = new BufferedReader(new InputStreamReader(in));
      String strLine;
      int count = 0;
      while ((strLine = br.readLine()) != null) {
        Assert.assertEquals("Comparing the values", val, strLine);
        count++;
      }
      return count;
    }
    catch (Exception e) {
      return -1;
    }
    finally {
      if (br != null) {
        try {
          br.close();
        }
        catch (IOException e) {
        }
      }
    }
  }

  private boolean checkFile(String path)
  {
    File file = new File(path);
    if (file.exists()) {
      file.delete();
      return true;
    }
    return false;
  }

}

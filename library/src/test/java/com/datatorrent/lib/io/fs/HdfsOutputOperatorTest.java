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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * This tests the HdfsOutput Operator.
 * 
 */
public class HdfsOutputOperatorTest
{

  static class MyStringClass implements HdfsOutputTupleInterface
  {

    String str;

    public MyStringClass(String str)
    {
      this.str = str;
    }

    @Override
    public byte[] getBytes()
    {
      return (str + "\n").getBytes();
    }
  }

  static class HdfsOutputOperator extends AbstractTupleHDFSOutputOperator<MyStringClass>
  {

    /**
     * File name substitution parameter: The logical id assigned to the operator when assembling the DAG.
     */
    public static final String FNAME_SUB_OPERATOR_ID = "operatorId";
    /**
     * File name substitution parameter: Index of part file when a file size limit is specified.
     */
    public static final String FNAME_SUB_PART_INDEX = "partIndex";

    private int operatorId;
    private int index = 0;

    @Override
    public Path nextFilePath()
    {
      Map<String, String> params = new HashMap<String, String>();
      params.put(FNAME_SUB_PART_INDEX, String.valueOf(index));
      params.put(FNAME_SUB_OPERATOR_ID, Integer.toString(operatorId));
      StrSubstitutor sub = new StrSubstitutor(params, "%(", ")");
      index++;
      return new Path(sub.replace(getFilePathPattern().toString()));
    }

    @Override
    public void setup(OperatorContext context)
    {
      operatorId = context.getId();
      super.setup(context);
    }
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
    } catch (Exception e) {
      return -1;
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
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

  @Test
  public void TestSeparateFilesEachWindow()
  {
    HdfsOutputOperator oper = new HdfsOutputOperator();
    oper.setFilePathPattern("target/file-%(operatorId)-%(partIndex)");
    oper.setCloseCurrentFile(true);
    oper.setAppend(false);
    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0));
    oper.beginWindow(0);
    oper.input.process(new MyStringClass("test"));
    oper.input.process(new MyStringClass("test"));
    oper.endWindow();
    oper.beginWindow(1);
    oper.input.process(new MyStringClass("test"));
    oper.input.process(new MyStringClass("test"));
    oper.endWindow();
    oper.teardown();
    Assert.assertEquals("The number of lines in file target/file-0-0", 2, readFile("target/file-0-0", "test"));
    Assert.assertEquals("The number of lines in file target/file-0-1", 2, readFile("target/file-0-1", "test"));
    Assert.assertEquals("Checking the file target/file-0-0", true, checkFile("target/file-0-0"));
    Assert.assertEquals("Checking the file target/file-0-1", true, checkFile("target/file-0-1"));
  }

  @Test
  public void TestSameFilesEachWindowWithReplacablePattern()
  {
    HdfsOutputOperator oper = new HdfsOutputOperator();
    oper.setFilePathPattern("target/file-%(operatorId)-%(partIndex)");
    oper.setCloseCurrentFile(false);
    oper.setAppend(true);
    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0));
    oper.beginWindow(0);
    oper.input.process(new MyStringClass("test"));
    oper.input.process(new MyStringClass("test"));
    oper.endWindow();
    oper.beginWindow(1);
    oper.input.process(new MyStringClass("test"));
    oper.input.process(new MyStringClass("test"));
    oper.endWindow();
    oper.teardown();
    Assert.assertEquals("The number of lines in file target/file-0-0", 4, readFile("target/file-0-0", "test"));
    Assert.assertEquals("Checking the file target/file-0-0", true, checkFile("target/file-0-0"));
    Assert.assertEquals("Checking the file target/file-0-1", false, checkFile("target/file-0-1"));
  }

  @Test
  public void TestSameFilesEachWindow()
  {
    HdfsOutputOperator oper = new HdfsOutputOperator();
    oper.setFilePathPattern("target/file");
    oper.setCloseCurrentFile(false);
    oper.setAppend(true);
    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0));
    oper.beginWindow(0);
    oper.input.process(new MyStringClass("test"));
    oper.input.process(new MyStringClass("test"));
    oper.endWindow();
    oper.beginWindow(1);
    oper.input.process(new MyStringClass("test"));
    oper.input.process(new MyStringClass("test"));
    oper.endWindow();
    oper.teardown();
    Assert.assertEquals("The number of lines in file target/file", 4, readFile("target/file", "test"));
    Assert.assertEquals("Checking the file target/file", true, checkFile("target/file"));
  }

  @Test
  public void TestSeparateFilesEachWindowFailure()
  {
    HdfsOutputOperator oper = new HdfsOutputOperator();
    oper.setFilePathPattern("target/file");
    oper.setCloseCurrentFile(true);
    oper.setAppend(false);
    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0));
    oper.beginWindow(0);
    oper.input.process(new MyStringClass("test"));
    oper.input.process(new MyStringClass("test"));
    oper.endWindow();
    try {
      oper.beginWindow(1);
    } catch (Exception e) {
      Assert.assertEquals("Exception", "Rolling files require %() placeholders for unique names: target/file", e.getMessage());
    }
    oper.teardown();
    Assert.assertEquals("The number of lines in file target/file", 2, readFile("target/file", "test"));
    Assert.assertEquals("Checking the file target/file", true, checkFile("target/file"));

  }
}

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

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.Context.Counters;
import com.datatorrent.api.Context.OperatorContext;

/**
 * Functional Test for {@link HdfsExactlyOnceOutputOperator}
 */
public class HdfsExactlyOnceOutputOperatorTest
{
  @Before
  public void setup()
  {
    deleteFile("target/0");
    deleteFile("target/1");
    deleteFile("target/2");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOperator()
  {
    HdfsExactlyOnceOutputOperator oper = new HdfsExactlyOnceOutputOperator();
    oper.setFilePath("target");
    oper.setup(new DummyContext(0));
    oper.beginWindow(0);
    oper.input.process("window 0");
    oper.input.process("window 0");
    oper.endWindow();

    oper.beginWindow(1);
    oper.input.process("window 1");
    oper.teardown();
    Assert.assertEquals("The number of lines in file target/0", 2, readFile("target/0", "window 0"));
    Assert.assertEquals("The number of lines in file target/1.tmp", 1, readFile("target/1.tmp", "window 1"));

    Assert.assertEquals("Checking the file target/1", false, checkFile("target/1"));

    oper.setup(new DummyContext(0));
    oper.beginWindow(0);
    oper.input.process("window_new 0");
    oper.input.process("window_new 0");
    oper.endWindow();

    oper.beginWindow(1);
    oper.input.process("window_new 1");
    oper.input.process("window_new 1");
    oper.endWindow();

    oper.beginWindow(2);
    oper.input.process("window_new 2");
    oper.input.process("window_new 2");
    oper.endWindow();

    oper.teardown();

    Assert.assertEquals("The number of lines in file target/0", 2, readFile("target/0", "window 0"));
    Assert.assertEquals("The number of lines in file target/1", 2, readFile("target/1", "window_new 1"));
    Assert.assertEquals("The number of lines in file target/2", 2, readFile("target/2", "window_new 2"));
    Assert.assertEquals("Checking the file target/0", true, checkFile("target/0"));
    Assert.assertEquals("Checking the file target/1", true, checkFile("target/1"));
    Assert.assertEquals("Checking the file target/2", true, checkFile("target/2"));
  }

  @After
  public void tearDown()
  {
    deleteFile("target/0");
    deleteFile("target/1");
    deleteFile("target/2");
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
      return true;
    }
    return false;
  }

  private void deleteFile(String path)
  {
    File file = new File(path);
    if (file.exists()) {
      file.delete();
    }
  }

  public static class DummyContext implements OperatorContext
  {
    int id;
    String applicationPath;
    String applicationId;
    AttributeMap attributes;

    public DummyContext(int id)
    {
      this.id = id;
    }

    public DummyContext(int id, @Nonnull AttributeMap map)
    {
      this.id = id;
      this.attributes = map;
    }

    @Override
    public int getId()
    {
      return id;
    }

    @Override
    public void setCounters(Counters stats)
    {
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getValue(Attribute<T> key)
    {
      T value = attributes.get(key);
      if (value != null) {
        return value;
      }

      return null;
    }

    @Override
    public AttributeMap getAttributes()
    {
      return attributes;
    }

  }

}

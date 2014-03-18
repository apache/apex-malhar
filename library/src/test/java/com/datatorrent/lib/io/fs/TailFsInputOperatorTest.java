/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional tests for {@link com.datatorrent.lib.io.localfs.TailFsInputOperator}
 * <p>
 */
public class TailFsInputOperatorTest
{

  private String filePath = "target/tailFsInputOperator.txt";

  @Test
  public void testTailInputOperator() throws Exception
  {
    FileWriter fstream = new FileWriter(filePath);
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("Hello Java\n");
    out.close();

    TailFsInputOperator oper = new TailFsInputOperator();
    oper.setFilePath(filePath);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.output.setSink(sink);
    oper.setDelay(1);
    oper.setNumberOfTuples(10);
    oper.setup(null);
    oper.activate(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    fstream = new FileWriter(filePath, true);
    out = new BufferedWriter(fstream);
    out.write("Hello Java\n");
    out.close();
    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();
    oper.deactivate();
    Assert.assertEquals(2, sink.collectedTuples.size());
    out.close();
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testTailFromEnd() throws Exception
  {
    FileWriter fstream = new FileWriter(filePath);
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("Hello Java");
    out.close();

    TailFsInputOperator oper = new TailFsInputOperator();
    oper.setFilePath(filePath);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.output.setSink(sink);
    oper.setDelay(1);
    oper.setEnd(true);
    oper.setNumberOfTuples(10);
    oper.setup(null);
    oper.activate(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    fstream = new FileWriter(filePath, true);
    out = new BufferedWriter(fstream);
    out.write("Hello Java\n");
    out.close();
    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();
    oper.deactivate();
    Assert.assertEquals(1, sink.collectedTuples.size());
    Assert.assertEquals("Hello Java", sink.collectedTuples.get(0));
    out.close();
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testDelimiter() throws Exception
  {
    FileWriter fstream = new FileWriter(filePath);
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("Hello Java");
    out.close();

    TailFsInputOperator oper = new TailFsInputOperator();
    oper.setFilePath(filePath);
    oper.setDelimiter('|');
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.output.setSink(sink);
    oper.setDelay(1);
    oper.setNumberOfTuples(10);
    oper.setup(null);
    oper.activate(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    fstream = new FileWriter(filePath, true);
    out = new BufferedWriter(fstream);
    out.write("Hello Java|");
    out.close();
    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();
    oper.deactivate();
    Assert.assertEquals(1, sink.collectedTuples.size());
    Assert.assertEquals("Hello JavaHello Java", sink.collectedTuples.get(0));
    out.close();
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testTruncation() throws Exception
  {
    FileWriter fstream = new FileWriter(filePath);
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("Hello Java");
    out.close();
    TailFsInputOperator oper = new TailFsInputOperator();
    oper.setFilePath(filePath);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.output.setSink(sink);
    oper.setDelay(1);
    oper.setEnd(true);
    oper.setNumberOfTuples(10);
    oper.setup(null);
    oper.activate(null);
    File file = new File(filePath);
    if (file.exists()) {
      file.renameTo(new File(filePath+".bk"));      
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
    fstream = new FileWriter(filePath);
    out = new BufferedWriter(fstream);
    out.write("Hello\n");
    out.close();
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.deactivate();
    file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
    file = new File(filePath + ".bk");
    if (file.exists()) {
      file.delete();
    }
    Assert.assertEquals(1, sink.collectedTuples.size());
    Assert.assertEquals("Hello", sink.collectedTuples.get(0));
  }

  /**
   * This tests the case when the file is rotated and new file has same size as old file
   * 
   * @throws Exception
   */

  @Test
  public void testTruncationWithSameFileSize() throws Exception
  {
    FileWriter fstream = new FileWriter(filePath);
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("Hello Java\n");
    out.close();
    TailFsInputOperator oper = new TailFsInputOperator();
    oper.setFilePath(filePath);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.output.setSink(sink);
    oper.setDelay(1);
    oper.setEnd(true);
    oper.setNumberOfTuples(10);
    oper.setup(null);
    oper.activate(null);
    File file = new File(filePath);
    if (file.exists()) {
      file.renameTo(new File(filePath+".bk"));      
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
    fstream = new FileWriter(filePath);
    out = new BufferedWriter(fstream);
    out.write("Hello abcd\n");
    out.close();
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.deactivate();
    file = new File(filePath);
    if (file.exists()) {
      file.delete();      
    }
    file = new File(filePath + ".bk");
    if (file.exists()) {
      file.delete();
    }
    Assert.assertEquals(1, sink.collectedTuples.size());
    Assert.assertEquals("Hello abcd", sink.collectedTuples.get(0));

  }

}

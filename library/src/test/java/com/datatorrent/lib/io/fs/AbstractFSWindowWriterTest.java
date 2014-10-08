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

import com.datatorrent.lib.io.fs.AbstractFSWriterTest.CheckPointWriter;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import java.io.*;
import org.junit.*;

/**
 * Functional Test for {@link AbstractFSWindowWriter}
 */
public class AbstractFSWindowWriterTest
{
  @Rule public TestInfo testMeta = new TestInfo();

  private void prepareTest() {
    deleteFile(testMeta.getDir() + "/0");
    deleteFile(testMeta.getDir() + "/1");
    deleteFile(testMeta.getDir() + "/2");
  }

  public static class FSWindowWriterString extends AbstractFSWindowWriter<String, String>
  {
    @Override
    protected byte[] getBytesForTuple(String tuple)
    {
      return (tuple + "\n").getBytes();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOperator()
  {
    prepareTest();
    AbstractFSWindowWriter oper = new FSWindowWriterString();
    oper.setFilePath(testMeta.getDir());
    oper.setup(new DummyContext(0));
    oper.beginWindow(0);
    oper.input.process("window 0");
    oper.input.process("window 0");
    oper.endWindow();

    CheckPointWriter checkPoint = AbstractFSWriterTest.checkpoint(oper);

    oper.beginWindow(1);
    oper.input.process("window 1");
    oper.teardown();

    AbstractFSWriterTest.restoreCheckPoint(checkPoint, oper);

    oper.setup(new DummyContext(0));

    oper.beginWindow(1);
    oper.input.process("window_new 1");
    oper.input.process("window_new 1");
    oper.endWindow();

    oper.beginWindow(2);
    oper.input.process("window_new 2");
    oper.input.process("window_new 2");
    oper.endWindow();

    oper.teardown();

    AbstractFSWriterTest.checkOutput(-1,
                                     testMeta.getDir() + "/" + "0",
                                     "window 0\n" +
                                     "window 0\n");

    AbstractFSWriterTest.checkOutput(-1,
                                     testMeta.getDir() + "/" + "1",
                                     "window_new 1\n" +
                                     "window_new 1\n");

    AbstractFSWriterTest.checkOutput(-1,
                                     testMeta.getDir() + "/" + "2",
                                     "window_new 2\n" +
                                     "window_new 2\n");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOperatorMidWindowRestore()
  {
    prepareTest();
    AbstractFSWindowWriter oper = new FSWindowWriterString();
    oper.setFilePath(testMeta.getDir());
    oper.setup(new DummyContext(0));
    oper.beginWindow(0);
    oper.input.process("0");
    oper.input.process("0");
    oper.endWindow();

    oper.beginWindow(1);
    oper.input.process("1");

    CheckPointWriter checkPoint = AbstractFSWriterTest.checkpoint(oper);

    oper.input.process("1");
    oper.teardown();

    AbstractFSWriterTest.restoreCheckPoint(checkPoint, oper);

    oper.setup(new DummyContext(0));

    oper.input.process("1");
    oper.input.process("1");
    oper.endWindow();

    oper.beginWindow(2);
    oper.input.process("2");
    oper.input.process("2");
    oper.endWindow();

    oper.teardown();

    AbstractFSWriterTest.checkOutput(-1,
                                     testMeta.getDir() + "/" + "0",
                                     "0\n" +
                                     "0\n");

    AbstractFSWriterTest.checkOutput(-1,
                                     testMeta.getDir() + "/" + "1",
                                     "1\n" +
                                     "1\n" +
                                     "1\n");

    AbstractFSWriterTest.checkOutput(-1,
                                     testMeta.getDir() + "/" + "2",
                                     "2\n" +
                                     "2\n");
  }

  private void deleteFile(String path)
  {
    File file = new File(path);
    if (file.exists()) {
      file.delete();
    }
  }
}

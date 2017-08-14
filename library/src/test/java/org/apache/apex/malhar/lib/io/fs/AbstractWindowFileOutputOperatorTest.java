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
package org.apache.apex.malhar.lib.io.fs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import org.apache.apex.malhar.lib.util.TestUtils.TestInfo;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.Stateless;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Functional Test for {@link AbstractWindowFileOutputOperator}
 */
public class AbstractWindowFileOutputOperatorTest
{
  @Rule
  public TestInfo testMeta = new PrivateTestWatcher();

  private static WindowFileOutputOperatorString oper;

  private static class PrivateTestWatcher extends FSTestWatcher
  {
    @Override
    public void starting(Description description)
    {
      super.starting(description);
      oper = new WindowFileOutputOperatorString();
      oper.setFilePath(getDir());
      oper.setAlwaysWriteToTmp(false);
      oper.setup(testOperatorContext);
    }

  }

  public static OperatorContext testOperatorContext = mockOperatorContext(0);

  public static class WindowFileOutputOperatorString extends AbstractWindowFileOutputOperator<String>
  {
    @Override
    protected byte[] getBytesForTuple(String tuple)
    {
      return (tuple + "\n").getBytes();
    }
  }

  @Test
  public void testOperator()
  {
    oper.beginWindow(0);
    oper.input.process("window 0");
    oper.input.process("window 0");
    oper.endWindow();

    AbstractFileOutputOperator checkPoint = AbstractFileOutputOperatorTest.checkpoint(oper, Stateless.WINDOW_ID);

    oper.beginWindow(1);
    oper.input.process("window 1");
    oper.teardown();

    AbstractFileOutputOperatorTest.restoreCheckPoint(checkPoint, oper);

    oper.setup(testOperatorContext);

    oper.beginWindow(1);
    oper.input.process("window_new 1");
    oper.input.process("window_new 1");
    oper.endWindow();

    oper.beginWindow(2);
    oper.input.process("window_new 2");
    oper.input.process("window_new 2");
    oper.endWindow();

    oper.teardown();

    AbstractFileOutputOperatorTest.checkOutput(-1, testMeta.getDir() + "/" + "0", "window 0\n" + "window 0\n");

    AbstractFileOutputOperatorTest.checkOutput(-1, testMeta.getDir() + "/" + "1", "window_new 1\n" + "window_new 1\n");

    AbstractFileOutputOperatorTest.checkOutput(-1, testMeta.getDir() + "/" + "2", "window_new 2\n" + "window_new 2\n");
  }

  @Test
  public void testOperatorMidWindowRestore()
  {
    oper.beginWindow(0);
    oper.input.process("0");
    oper.input.process("0");
    oper.endWindow();

    oper.beginWindow(1);
    oper.input.process("1");

    AbstractFileOutputOperator checkPoint = AbstractFileOutputOperatorTest.checkpoint(oper, Stateless.WINDOW_ID);

    oper.input.process("1");
    oper.teardown();

    AbstractFileOutputOperatorTest.restoreCheckPoint(checkPoint, oper);

    oper.setup(testOperatorContext);

    oper.input.process("1");
    oper.input.process("1");
    oper.endWindow();

    oper.beginWindow(2);
    oper.input.process("2");
    oper.input.process("2");
    oper.endWindow();

    oper.teardown();

    AbstractFileOutputOperatorTest.checkOutput(-1, testMeta.getDir() + "/" + "0", "0\n" + "0\n");

    AbstractFileOutputOperatorTest.checkOutput(-1, testMeta.getDir() + "/" + "1", "1\n" + "1\n" + "1\n");

    AbstractFileOutputOperatorTest.checkOutput(-1, testMeta.getDir() + "/" + "2", "2\n" + "2\n");
  }
}

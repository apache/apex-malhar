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

import com.datatorrent.api.*;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.google.common.collect.*;
import java.io.*;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.*;
import org.junit.*;

public class AbstractFileInputOperatorFailureHandlingTest
{
  @Rule public TestInfo testMeta = new TestInfo();

  public static class TestFileInputOperator extends AbstractFileInputOperator<String>
  {
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
    private transient BufferedReader br = null;
    int count = 0;

    @Override
    protected InputStream openFile(Path path) throws IOException
    {
      InputStream is = super.openFile(path);
      br = new BufferedReader(new InputStreamReader(is));
      count = 0;
      return is;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException
    {
      super.closeFile(is);
      br.close();
      count = 0;
      br = null;
    }

    @Override protected InputStream retryFailedFile(FailedFile ff) throws IOException
    {
      count = 0;
      return super.retryFailedFile(ff);
    }

    @Override
    protected String readEntity() throws IOException
    {
      if (count != 0 && (count % 4) == 0) {
        addToFailedList();
        return null;
      }
      return br.readLine();
    }

    @Override
    protected void emit(String tuple)
    {
      output.emit(tuple);
      count++;
    }
  }

  @Test
  public void testFailureHandling() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.getDir()).getAbsolutePath()), true);
    HashSet<String> allLines = Sets.newHashSet();
    // Create files with 100 records.
    for (int file=0; file<10; file++) {
      HashSet<String> lines = Sets.newHashSet();
      for (int line=0; line<10; line++) {
        lines.add("f"+file+"l"+line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.getDir(), "file"+file), StringUtils.join(lines, '\n'));
    }

    Thread.sleep(10);

    TestFileInputOperator oper = new TestFileInputOperator();

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.getDir());
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(1, new Attribute.AttributeMap.DefaultAttributeMap()));
    for (long wid=0; wid<1000; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();

    Assert.assertEquals("number tuples", 100, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines, new HashSet<String>(queryResults.collectedTuples));

  }
}

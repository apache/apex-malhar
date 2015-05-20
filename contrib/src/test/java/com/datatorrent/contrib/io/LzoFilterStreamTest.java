/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.contrib.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.anarres.lzo.LzopInputStream;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.util.TestUtils.TestInfo;

public class LzoFilterStreamTest
{
  public static OperatorContextTestHelper.TestIdOperatorContext testOperatorContext = new OperatorContextTestHelper.TestIdOperatorContext(0);
  @Rule
  public TestInfo testMeta = new FSTestWatcher();

  public static class FSTestWatcher extends TestInfo
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      new File(getDir()).mkdir();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(getDir()));
    }
  }

  private class FileOutputOperator extends AbstractFileOutputOperator<Integer>
  {
    private String OUTPUT_FILENAME = "outputdata.txt";

    public FileOutputOperator(String outputFileName)
    {
      OUTPUT_FILENAME = outputFileName;
    }

    @Override
    protected String getFileName(Integer tuple)
    {
      return OUTPUT_FILENAME;
    }

    @Override
    protected byte[] getBytesForTuple(Integer tuple)
    {
      return (tuple.toString() + "\n").getBytes();
    }
  }

  @Test
  public void testLZOCompression() throws Exception
  {
    FileOutputOperator writer = new FileOutputOperator("compressedData.txt.lzo");
    writer.setFilterStreamProvider(new LzoFilterStream.LZOFilterStreamProvider());

    writer.setFilePath(testMeta.getDir());
    writer.setup(testOperatorContext);

    for (int i = 0; i < 10; ++i) {
      writer.beginWindow(i);
      writer.input.put(i);
      writer.endWindow();
    }
    writer.teardown();

    // test compressed data
    File compressedFile = new File(testMeta.getDir() + File.separator + writer.OUTPUT_FILENAME);
    LzopInputStream lzoInputStream = new LzopInputStream(new FileInputStream(compressedFile));
    BufferedReader br = new BufferedReader(new InputStreamReader(lzoInputStream));
    try {
      String fline;
      int expectedInt = 0;
      while ((fline = br.readLine()) != null) {
        Assert.assertEquals("File line", Integer.toString(expectedInt++), fline);
      }
    } finally {
      br.close();
      lzoInputStream.close();
    }
  }

}

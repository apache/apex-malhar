package com.datatorrent.contrib.lib.io;

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

public class FilterStreamCodecTest
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
    writer.setFilterStreamProvider(new FilterStreamCodec.LZOFilterStreamProvider());

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

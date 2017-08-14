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

package org.apache.apex.malhar.lib.fs.s3;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.apex.malhar.lib.fs.FSRecordReaderTest.DelimitedValidator;
import org.apache.apex.malhar.lib.fs.FSRecordReaderTest.FixedWidthValidator;
import org.apache.apex.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import org.apache.apex.malhar.lib.io.block.ReaderContext;
import org.apache.apex.malhar.lib.io.fs.S3BlockReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.AmazonS3;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

import static org.mockito.Mockito.mock;

public class S3RecordReaderMockTest
{
  private String inputDir;
  static String outputDir;
  private static final String FILE_1 = "file1.txt";
  private static final String FILE_2 = "file2.txt";
  private static final String FILE_1_DATA = "1234\n567890\nabcde\nfgh\ni\njklmop";
  private static final String FILE_2_DATA = "qr\nstuvw\nxyz\n";

  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Before
  public void setup() throws Exception
  {
    inputDir = testMeta.baseDirectory + File.separator + "input";

    FileUtils.writeStringToFile(new File(inputDir + File.separator + FILE_1), FILE_1_DATA);
    FileUtils.writeStringToFile(new File(inputDir + File.separator + FILE_2), FILE_2_DATA);
  }

  @Test
  public void testDelimitedRecords() throws Exception
  {

    DelimitedApplication app = new DelimitedApplication();
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.files", inputDir);
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.blockSize", "3");
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.blocksThreshold", "1");
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.scanIntervalMillis", "10000");

    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    Set<String> expectedRecords = new HashSet<String>(Arrays.asList(FILE_1_DATA.split("\n")));
    expectedRecords.addAll(Arrays.asList(FILE_2_DATA.split("\n")));

    while (DelimitedValidator.getRecords().size() != expectedRecords.size()) {
      LOG.debug("Waiting for app to finish");
      Thread.sleep(1000);
    }
    lc.shutdown();
    Assert.assertEquals(expectedRecords, DelimitedValidator.getRecords());

  }

  private static class S3RecordReaderMock extends S3RecordReader
  {
    AmazonS3 s3ClientObject;

    @Override
    protected FSDataInputStream setupStream(FileBlockMetadata block) throws IOException
    {
      super.setupStream(block);
      return fs.open(new Path(block.getFilePath()));
    }

    @Override
    public void setup(OperatorContext context)
    {
      s3ClientObject = mock(AmazonS3.class);
      super.setup(context);
    }

    @Override
    protected ReaderContext<FSDataInputStream> createDelimitedReaderContext()
    {
      S3DelimitedRecordReaderContextMock s3DelimitedRecordReaderContextMock = new S3DelimitedRecordReaderContextMock();
      s3DelimitedRecordReaderContextMock.getS3Params().setBucketName("S3RecordReaderMock");
      s3DelimitedRecordReaderContextMock.getS3Params().setS3Client(s3ClientObject);
      return s3DelimitedRecordReaderContextMock;
    }

    @Override
    protected ReaderContext<FSDataInputStream> createFixedWidthReaderContext()
    {
      S3FixedWidthRecordReaderContextMock s3FixedWidthRecordReaderContextMock = new S3FixedWidthRecordReaderContextMock();
      s3FixedWidthRecordReaderContextMock.getS3Params().setBucketName("S3RecordReaderMock");
      s3FixedWidthRecordReaderContextMock.getS3Params().setS3Client(s3ClientObject);
      s3FixedWidthRecordReaderContextMock.setLength(this.getRecordLength());
      return s3FixedWidthRecordReaderContextMock;
    }

    private class S3DelimitedRecordReaderContextMock extends S3DelimitedRecordReaderContext
    {
      @Override
      protected int readData(long bytesFromCurrentOffset, int bytesToFetch) throws IOException
      {
        if (buffer == null) {
          buffer = new byte[bytesToFetch];
        }
        return stream.read(offset + bytesFromCurrentOffset, buffer, 0, bytesToFetch);
      }
    }

    private static class S3FixedWidthRecordReaderContextMock extends S3FixedWidthRecordReaderContext
    {
      @Override
      protected int readData(long startOffset, long endOffset) throws IOException
      {
        int bufferSize = Long.valueOf(endOffset - startOffset + 1).intValue();
        if (buffer == null) {
          buffer = new byte[bufferSize];
        }
        return stream.read(startOffset, buffer, 0, bufferSize);
      }
    }
  }

  private static class S3RecordReaderModuleMock extends S3RecordReaderModule
  {
    @Override
    public S3RecordReader createRecordReader()
    {
      S3RecordReader s3RecordReader = new S3RecordReaderMock();
      s3RecordReader.setBucketName(S3BlockReader.extractBucket(getFiles()));
      s3RecordReader.setAccessKey("****");
      s3RecordReader.setSecretAccessKey("*****");
      s3RecordReader.setMode(this.getMode().toString());
      s3RecordReader.setRecordLength(this.getRecordLength());
      return s3RecordReader;
    }
  }

  private static class DelimitedApplication implements StreamingApplication
  {

    public void populateDAG(DAG dag, Configuration conf)
    {

      S3RecordReaderModuleMock recordReader = dag.addModule("S3RecordReaderModuleMock", new S3RecordReaderModuleMock());
      recordReader.setMode("delimited_record");
      DelimitedValidator validator = dag.addOperator("Validator", new DelimitedValidator());
      dag.addStream("records", recordReader.records, validator.data);
    }

  }

  @Test
  public void testFixedWidthRecords() throws Exception
  {

    FixedWidthApplication app = new FixedWidthApplication();
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.files", inputDir);
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.recordLength", "8");
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.blockSize", "3");
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.blocksThreshold", "1");
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.scanIntervalMillis", "10000");

    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();
    LOG.debug("Waiting for app to finish");
    Thread.sleep(1000 * 1);
    lc.shutdown();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingRecordLength() throws Exception
  {
    FixedWidthApplication app = new FixedWidthApplication();
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.files", inputDir);
    //Should give IllegalArgumentException since recordLength is not set
    //conf.set("dt.operator.HDFSRecordReaderModule.prop.recordLength", "8");
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.blocksThreshold", "1");
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.blockSize", "3");
    conf.set("dt.operator.S3RecordReaderModuleMock.prop.scanIntervalMillis", "10000");

    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();
    LOG.debug("Waiting for app to finish");
    Thread.sleep(1000 * 1);
    lc.shutdown();
  }

  private static class FixedWidthApplication implements StreamingApplication
  {

    public void populateDAG(DAG dag, Configuration conf)
    {
      FSRecordReaderModule recordReader = dag.addModule("S3RecordReaderModuleMock", FSRecordReaderModule.class);
      recordReader.setMode("FIXED_WIDTH_RECORD");
      FixedWidthValidator validator = dag.addOperator("Validator", new FixedWidthValidator());
      dag.addStream("records", recordReader.records, validator.data);
    }

  }

  private static Logger LOG = LoggerFactory.getLogger(S3RecordReaderMockTest.class);

}

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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.block.AbstractBlockReader.ReaderRecord;
import org.apache.apex.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import org.apache.apex.malhar.lib.io.fs.AbstractFileSplitter.FileMetadata;
import org.apache.apex.malhar.lib.stream.DevNull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.netlet.util.Slice;

public class FSInputModuleAppTest
{
  private String inputDir;
  static String outputDir;
  private StreamingApplication app;
  private static final String FILE_1 = "file1.txt";
  private static final String FILE_2 = "file2.txt";
  private static final String FILE_1_DATA = "File one data";
  private static final String FILE_2_DATA = "File two data. This has more data hence more blocks.";
  static final String OUT_DATA_FILE = "fileData.txt";
  static final String OUT_METADATA_FILE = "fileMetaData.txt";

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
    outputDir = testMeta.baseDirectory + File.separator + "output";

    FileUtils.writeStringToFile(new File(inputDir + File.separator + FILE_1), FILE_1_DATA);
    FileUtils.writeStringToFile(new File(inputDir + File.separator + FILE_2), FILE_2_DATA);
    FileUtils.forceMkdir(new File(inputDir + File.separator + "dir"));
    FileUtils.writeStringToFile(new File(inputDir + File.separator + "dir/inner.txt"), FILE_1_DATA);
  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(new File(inputDir));
  }

  @Test
  public void testApplication() throws Exception
  {
    app = new Application();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.hdfsInputModule.prop.files", inputDir);
    conf.set("dt.operator.hdfsInputModule.prop.blockSize", "10");
    conf.set("dt.operator.hdfsInputModule.prop.blocksThreshold", "4");
    conf.set("dt.operator.hdfsInputModule.prop.scanIntervalMillis", "10000");
    conf.set("dt.attr.CHECKPOINT_WINDOW_COUNT","10");

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    long now = System.currentTimeMillis();
    Path outDir = new Path("file://" + new File(outputDir).getAbsolutePath());
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }

    Thread.sleep(10000);
    lc.shutdown();

    Assert.assertTrue("output dir does not exist", fs.exists(outDir));

    File dir = new File(outputDir);
    FileFilter fileFilter = new WildcardFileFilter(OUT_METADATA_FILE + "*");
    verifyFileContents(dir.listFiles(fileFilter), "[fileName=file1.txt, numberOfBlocks=2, isDirectory=false, relativePath=input/file1.txt]");
    verifyFileContents(dir.listFiles(fileFilter), "[fileName=file2.txt, numberOfBlocks=6, isDirectory=false, relativePath=input/file2.txt]");
    verifyFileContents(dir.listFiles(fileFilter), "[fileName=dir, numberOfBlocks=0, isDirectory=true, relativePath=input/dir]");
    verifyFileContents(dir.listFiles(fileFilter), "[fileName=inner.txt, numberOfBlocks=2, isDirectory=false, relativePath=input/dir/inner.txt]");

    fileFilter = new WildcardFileFilter(OUT_DATA_FILE + "*");
    verifyFileContents(dir.listFiles(fileFilter), FILE_1_DATA);
    verifyFileContents(dir.listFiles(fileFilter), FILE_2_DATA);
  }

  private void verifyFileContents(File[] files, String expectedData) throws IOException
  {
    StringBuilder filesData = new StringBuilder();
    for (File file : files) {
      filesData.append(FileUtils.readFileToString(file));
    }
    Assert.assertTrue("File data doesn't contain expected text", filesData.indexOf(expectedData) > -1);
  }

  private static Logger LOG = LoggerFactory.getLogger(FSInputModuleAppTest.class);

  private static class Application implements StreamingApplication
  {
    public void populateDAG(DAG dag, Configuration conf)
    {
      FSInputModule module = dag.addModule("hdfsInputModule", FSInputModule.class);

      AbstractFileOutputOperator<FileMetadata> metadataWriter = new MetadataWriter(FSInputModuleAppTest.OUT_METADATA_FILE);
      metadataWriter.setFilePath(FSInputModuleAppTest.outputDir);
      dag.addOperator("FileMetadataWriter", metadataWriter);

      AbstractFileOutputOperator<ReaderRecord<Slice>> dataWriter = new HDFSFileWriter(FSInputModuleAppTest.OUT_DATA_FILE);
      dataWriter.setFilePath(FSInputModuleAppTest.outputDir);
      dag.addOperator("FileDataWriter", dataWriter);

      DevNull<FileBlockMetadata> devNull = dag.addOperator("devNull", DevNull.class);

      dag.addStream("FileMetaData", module.filesMetadataOutput, metadataWriter.input);
      dag.addStream("data", module.messages, dataWriter.input);
      dag.addStream("blockMetadata", module.blocksMetadataOutput, devNull.data);
    }
  }

  private static class MetadataWriter extends AbstractFileOutputOperator<FileMetadata>
  {
    String fileName;

    @SuppressWarnings("unused")
    private MetadataWriter()
    {

    }

    public MetadataWriter(String fileName)
    {
      this.fileName = fileName;
    }

    @Override
    protected String getFileName(FileMetadata tuple)
    {
      return fileName;
    }

    @Override
    protected byte[] getBytesForTuple(FileMetadata tuple)
    {
      return (tuple).toString().getBytes();
    }
  }

  private static class HDFSFileWriter extends AbstractFileOutputOperator<ReaderRecord<Slice>>
  {
    String fileName;

    @SuppressWarnings("unused")
    private HDFSFileWriter()
    {
    }

    public HDFSFileWriter(String fileName)
    {
      this.fileName = fileName;
    }

    @Override
    protected String getFileName(ReaderRecord<Slice> tuple)
    {
      return fileName;
    }

    @Override
    protected byte[] getBytesForTuple(ReaderRecord<Slice> tuple)
    {
      return tuple.getRecord().buffer;
    }
  }

}

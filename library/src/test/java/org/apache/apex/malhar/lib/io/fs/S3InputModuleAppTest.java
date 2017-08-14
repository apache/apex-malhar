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
import java.util.Iterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.block.AbstractBlockReader;
import org.apache.apex.malhar.lib.io.block.BlockMetadata;
import org.apache.apex.malhar.lib.stream.DevNull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.netlet.util.Slice;

@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3InputModuleAppTest
{
  private String inputDir;
  static String outputDir;
  private StreamingApplication app;
  private String accessKey = "*************";
  private String secretKey = "**************";
  private AmazonS3 client;
  private String files;
  private static final String SCHEME = "s3n";
  private static final String FILE_1 = "file1.txt";
  private static final String FILE_2 = "file2.txt";
  private static final String FILE_1_DATA = "File one data";
  private static final String FILE_2_DATA = "File two data. This has more data hence more blocks.";
  static final String OUT_DATA_FILE = "fileData.txt";
  static final String OUT_METADATA_FILE = "fileMetaData.txt";

  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;
    public String bucketKey;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
      this.bucketKey = new String("target-" + description.getMethodName()).toLowerCase();
    }

  }

  @Rule
  public S3InputModuleAppTest.TestMeta testMeta = new S3InputModuleAppTest.TestMeta();

  @Before
  public void setup() throws Exception
  {
    client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    client.createBucket(testMeta.bucketKey);

    inputDir = testMeta.baseDirectory + File.separator + "input";
    outputDir = testMeta.baseDirectory + File.separator + "output";

    File file1 = new File(inputDir + File.separator + FILE_1);
    File file2 = new File(inputDir + File.separator + FILE_2);

    FileUtils.writeStringToFile(file1, FILE_1_DATA);
    FileUtils.writeStringToFile(file2, FILE_2_DATA);
    client.putObject(new PutObjectRequest(testMeta.bucketKey, "input/" + FILE_1, file1));
    client.putObject(new PutObjectRequest(testMeta.bucketKey, "input/" + FILE_2, file2));
    files = SCHEME + "://" + accessKey + ":" + secretKey + "@" + testMeta.bucketKey + "/input";
  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(new File(inputDir));
    FileUtils.deleteDirectory(new File(outputDir));
    deleteBucketAndContent();
    //client.deleteBucket(testMeta.bucketKey);
  }

  public void deleteBucketAndContent()
  {
    //Get the list of objects
    ObjectListing objectListing = client.listObjects(testMeta.bucketKey);
    for ( Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); ) {
      S3ObjectSummary objectSummary = (S3ObjectSummary)iterator.next();
      LOG.info("Deleting an object: {}",objectSummary.getKey());
      client.deleteObject(testMeta.bucketKey, objectSummary.getKey());
    }
    client.deleteBucket(testMeta.bucketKey);
  }

  @Test
  public void testS3Application() throws Exception
  {
    app = new S3InputModuleAppTest.Application();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.s3InputModule.prop.files", files);
    conf.set("dt.operator.s3InputModule.prop.blockSize", "10");
    conf.set("dt.operator.s3InputModule.prop.scanIntervalMillis", "10000");

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

  private static Logger LOG = LoggerFactory.getLogger(S3InputModuleAppTest.class);

  private static class Application implements StreamingApplication
  {
    public void populateDAG(DAG dag, Configuration conf)
    {
      S3InputModule module = dag.addModule("s3InputModule", S3InputModule.class);

      AbstractFileOutputOperator<AbstractFileSplitter.FileMetadata> metadataWriter = new S3InputModuleAppTest.MetadataWriter(S3InputModuleAppTest.OUT_METADATA_FILE);
      metadataWriter.setFilePath(S3InputModuleAppTest.outputDir);
      dag.addOperator("FileMetadataWriter", metadataWriter);

      AbstractFileOutputOperator<AbstractBlockReader.ReaderRecord<Slice>> dataWriter = new S3InputModuleAppTest.HDFSFileWriter(S3InputModuleAppTest.OUT_DATA_FILE);
      dataWriter.setFilePath(S3InputModuleAppTest.outputDir);
      dag.addOperator("FileDataWriter", dataWriter);

      DevNull<BlockMetadata.FileBlockMetadata> devNull = dag.addOperator("devNull", DevNull.class);

      dag.addStream("FileMetaData", module.filesMetadataOutput, metadataWriter.input);
      dag.addStream("data", module.messages, dataWriter.input);
      dag.addStream("blockMetadata", module.blocksMetadataOutput, devNull.data);
    }
  }

  private static class MetadataWriter extends AbstractFileOutputOperator<AbstractFileSplitter.FileMetadata>
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
    protected String getFileName(AbstractFileSplitter.FileMetadata tuple)
    {
      return fileName;
    }

    @Override
    protected byte[] getBytesForTuple(AbstractFileSplitter.FileMetadata tuple)
    {
      return (tuple).toString().getBytes();
    }
  }

  private static class HDFSFileWriter extends AbstractFileOutputOperator<AbstractBlockReader.ReaderRecord<Slice>>
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
    protected String getFileName(AbstractBlockReader.ReaderRecord<Slice> tuple)
    {
      return fileName;
    }

    @Override
    protected byte[] getBytesForTuple(AbstractBlockReader.ReaderRecord<Slice> tuple)
    {
      return tuple.getRecord().buffer;
    }
  }

}

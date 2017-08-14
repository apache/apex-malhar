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
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.helper.OperatorContextTestHelper;
import org.apache.apex.malhar.lib.io.fs.FSInputModule;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.StramLocalCluster;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Verifies the S3OutputModule using the application. This reads the data from local file system
 * "input" directory and uploads the files into "output" directory.
 */
public class S3OutputModuleMockTest
{
  private String uploadId = "uploadfile";
  private static final String APPLICATION_PATH_PREFIX = "target/s3outputmocktest/";
  private static final String FILE_DATA = "Testing the S3OutputModule. This File has more data hence more blocks.";
  private static final String FILE = "file.txt";
  private String inputDir;
  private String outputDir;
  private String applicationPath;
  private File inputFile;
  @Mock
  public static AmazonS3 client;

  @Before
  public void beforeTest() throws IOException
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    inputDir = applicationPath + File.separator + "input";
    outputDir = applicationPath + File.separator + "output";
    inputFile = new File(inputDir + File.separator + FILE);
    FileUtils.writeStringToFile(inputFile, FILE_DATA);
  }

  @After
  public void afterTest()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private CompleteMultipartUploadResult completeMultiPart() throws IOException
  {
    FileUtils.copyFile(inputFile, new File(outputDir + File.separator + FILE));
    CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
    result.setETag(outputDir);
    return result;
  }

  @Test
  public void testS3OutputModule() throws Exception
  {
    InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
    result.setUploadId(uploadId);

    PutObjectResult objResult = new PutObjectResult();
    objResult.setETag("SuccessFullyUploaded");

    UploadPartResult partResult = new UploadPartResult();
    partResult.setPartNumber(1);
    partResult.setETag("SuccessFullyPartUploaded");

    MockitoAnnotations.initMocks(this);
    when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(result);
    when(client.putObject(any(PutObjectRequest.class))).thenReturn(objResult);
    when(client.uploadPart(any(UploadPartRequest.class))).thenReturn(partResult);
    when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(completeMultiPart());

    Application app = new S3OutputModuleMockTest.Application();
    Configuration conf = new Configuration();
    conf.set("dt.operator.HDFSInputModule.prop.files", inputDir);
    conf.set("dt.operator.HDFSInputModule.prop.blockSize", "10");
    conf.set("dt.operator.HDFSInputModule.prop.blocksThreshold", "1");
    conf.set("dt.attr.CHECKPOINT_WINDOW_COUNT","20");

    conf.set("dt.operator.S3OutputModule.prop.accessKey", "accessKey");
    conf.set("dt.operator.S3OutputModule.prop.secretAccessKey", "secretKey");
    conf.set("dt.operator.S3OutputModule.prop.bucketName", "bucketKey");
    conf.set("dt.operator.S3OutputModule.prop.outputDirectoryPath", outputDir);

    Path outDir = new Path("file://" + new File(outputDir).getAbsolutePath());
    final Path outputFilePath =  new Path(outDir.toString() + File.separator + FILE);
    final FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return fs.exists(outputFilePath);
      }
    });
    lc.run(10000);

    Assert.assertTrue("output file exist", fs.exists(outputFilePath));
  }

  private static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      FSInputModule inputModule = dag.addModule("HDFSInputModule", new FSInputModule());
      S3OutputTestModule outputModule = dag.addModule("S3OutputModule", new S3OutputTestModule());

      dag.addStream("FileMetaData", inputModule.filesMetadataOutput, outputModule.filesMetadataInput);
      dag.addStream("BlocksMetaData", inputModule.blocksMetadataOutput, outputModule.blocksMetadataInput)
        .setLocality(DAG.Locality.CONTAINER_LOCAL);
      dag.addStream("BlocksData", inputModule.messages, outputModule.blockData).setLocality(DAG.Locality.CONTAINER_LOCAL);

    }
  }
}

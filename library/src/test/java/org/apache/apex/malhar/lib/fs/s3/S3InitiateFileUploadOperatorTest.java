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

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.helper.OperatorContextTestHelper;
import org.apache.apex.malhar.lib.io.fs.AbstractFileSplitter;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

/**
 * Testing the S3InitiateFileUploadOperator operator. It verifies the generated upload id by S3InitiateFileUploadOperator
 * and client through the mock are same or not.
 */
public class S3InitiateFileUploadOperatorTest
{
  private String uploadId = "uploadfile1";
  private static final String APPLICATION_PATH_PREFIX = "target/s3outputtest/";
  private String applicationPath;
  private Attribute.AttributeMap.DefaultAttributeMap attributes;
  private Context.OperatorContext context;
  @Mock
  public AmazonS3 client;
  @Mock
  public AbstractFileSplitter.FileMetadata fileMetadata;

  public class S3InitiateFileUploadTest extends S3InitiateFileUploadOperator
  {
    @Override
    protected AmazonS3 createClient()
    {
      return client;
    }
  }

  @Before
  public void beforeTest()
  {
    applicationPath =  OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    context = mockOperatorContext(1, attributes);
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

  @Test
  public void testInitiateUpload()
  {
    InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
    result.setUploadId(uploadId);

    MockitoAnnotations.initMocks(this);
    when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(result);
    when(fileMetadata.getFilePath()).thenReturn("/tmp/file1.txt");
    when(fileMetadata.getNumberOfBlocks()).thenReturn(4);

    S3InitiateFileUploadTest operator = new S3InitiateFileUploadTest();
    operator.setBucketName("testbucket");
    operator.setup(context);

    CollectorTestSink<S3InitiateFileUploadOperator.UploadFileMetadata> fileSink = new CollectorTestSink<>();
    CollectorTestSink<Object> tmp = (CollectorTestSink)fileSink;
    operator.fileMetadataOutput.setSink(tmp);
    operator.beginWindow(0);
    operator.processTuple(fileMetadata);
    operator.endWindow();

    S3InitiateFileUploadOperator.UploadFileMetadata emitted = (S3InitiateFileUploadOperator.UploadFileMetadata)tmp.collectedTuples.get(0);
    Assert.assertEquals("Upload ID :", uploadId, emitted.getUploadId());
  }
}

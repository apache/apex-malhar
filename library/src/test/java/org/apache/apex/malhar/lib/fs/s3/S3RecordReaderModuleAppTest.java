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
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.FSRecordReaderTest.DelimitedValidator;
import org.apache.apex.malhar.lib.fs.FSRecordReaderTest.FixedWidthValidator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

@Ignore
public class S3RecordReaderModuleAppTest
{
  private String inputDir;
  private static final String FILE_1 = "file1.txt";
  private static final String FILE_2 = "file2.txt";
  private static final String FILE_1_DATA = "1234\n567890\nabcde\nfgh\ni\njklmop";
  private static final String FILE_2_DATA = "qr\nstuvw\nxyz\n";

  private final String accessKey = "*************";
  private final String secretKey = "*********************";
  private AmazonS3 client;
  private String files;
  private static final String SCHEME = "s3n";

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
  public S3RecordReaderModuleAppTest.TestMeta testMeta = new S3RecordReaderModuleAppTest.TestMeta();

  @Before
  public void setup() throws Exception
  {
    client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    client.createBucket(testMeta.bucketKey);
    inputDir = testMeta.baseDirectory + File.separator + "input";

    File file1 = new File(inputDir + File.separator + FILE_1);
    File file2 = new File(inputDir + File.separator + FILE_2);

    FileUtils.writeStringToFile(file1, FILE_1_DATA);
    FileUtils.writeStringToFile(file2, FILE_2_DATA);

    client.putObject(new PutObjectRequest(testMeta.bucketKey, "input/" + FILE_1, file1));
    client.putObject(new PutObjectRequest(testMeta.bucketKey, "input/" + FILE_2, file2));
    files = SCHEME + "://" + accessKey + ":" + secretKey + "@" + testMeta.bucketKey + "/input";
  }

  @Test
  public void testS3DelimitedRecords() throws Exception
  {

    S3DelimitedApplication app = new S3DelimitedApplication();
    LocalMode lma = LocalMode.newInstance();

    Configuration conf = new Configuration(false);
    conf.set("dt.operator.s3RecordReaderModule.prop.files", files);
    conf.set("dt.operator.s3RecordReaderModule.prop.blockSize", "10");
    conf.set("dt.operator.s3RecordReaderModule.prop.overflowBlockSize", "4");
    conf.set("dt.operator.s3RecordReaderModule.prop.scanIntervalMillis", "10000");

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

  private static class S3DelimitedApplication implements StreamingApplication
  {

    public void populateDAG(DAG dag, Configuration conf)
    {
      S3RecordReaderModule recordReader = dag.addModule("s3RecordReaderModule", S3RecordReaderModule.class);
      DelimitedValidator validator = dag.addOperator("Validator", new DelimitedValidator());
      dag.addStream("records", recordReader.records, validator.data);
    }

  }

  @Test
  public void testS3FixedWidthRecords() throws Exception
  {

    S3FixedWidthApplication app = new S3FixedWidthApplication();
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.S3RecordReaderModule.prop.files", files);
    conf.set("dt.operator.S3RecordReaderModule.prop.recordLength", "8");
    conf.set("dt.operator.S3RecordReaderModule.prop.blocksThreshold", "1");
    conf.set("dt.operator.S3RecordReaderModule.prop.scanIntervalMillis", "10000");

    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();
    LOG.debug("Waiting for app to finish");
    Thread.sleep(1000 * 1);
    lc.shutdown();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testS3MissingRecordLength() throws Exception
  {
    S3FixedWidthApplication app = new S3FixedWidthApplication();
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.S3RecordReaderModule.prop.files", files);
    //Should give IllegalArgumentException since recordLength is not set
    //conf.set("dt.operator.S3RecordReaderModule.prop.recordLength", "8");
    conf.set("dt.operator.S3RecordReaderModule.prop.blocksThreshold", "1");
    conf.set("dt.operator.S3RecordReaderModule.prop.scanIntervalMillis", "10000");

    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();
    LOG.debug("Waiting for app to finish");
    Thread.sleep(1000 * 1);
    lc.shutdown();
  }

  private static class S3FixedWidthApplication implements StreamingApplication
  {

    public void populateDAG(DAG dag, Configuration conf)
    {
      S3RecordReaderModule recordReader = dag.addModule("S3RecordReaderModule", S3RecordReaderModule.class);
      recordReader.setMode("fixed_width_record");
      FixedWidthValidator validator = dag.addOperator("Validator", new FixedWidthValidator());
      dag.addStream("records", recordReader.records, validator.data);
    }

  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(new File(inputDir));
    deleteBucketAndContent();
  }

  public void deleteBucketAndContent()
  {
    //Get the list of objects
    ObjectListing objectListing = client.listObjects(testMeta.bucketKey);
    for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext();) {
      S3ObjectSummary objectSummary = (S3ObjectSummary)iterator.next();
      LOG.info("Deleting an object: {}", objectSummary.getKey());
      client.deleteObject(testMeta.bucketKey, objectSummary.getKey());
    }
    client.deleteBucket(testMeta.bucketKey);
  }

  private static Logger LOG = LoggerFactory.getLogger(S3RecordReaderModuleAppTest.class);
}

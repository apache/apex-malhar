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
import java.util.Collection;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class S3ReconcilerTest
{

  private class TestMeta extends TestWatcher
  {
    S3Reconciler underTest;
    Context.OperatorContext context;

    @Mock
    AmazonS3 s3clientMock;
    String outputPath;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      outputPath = new File(
          "target" + Path.SEPARATOR + description.getClassName() + Path.SEPARATOR + description.getMethodName())
              .getPath();

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getClassName());
      attributes.put(DAG.DAGContext.APPLICATION_PATH, outputPath);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

      underTest = new S3Reconciler();
      underTest.setAccessKey("");
      underTest.setSecretKey("");

      underTest.setup(context);

      MockitoAnnotations.initMocks(this);
      when(s3clientMock.putObject((PutObjectRequest)any())).thenReturn(null);
      underTest.setS3client(s3clientMock);
    }

    @Override
    protected void finished(Description description)
    {
      this.underTest.teardown();
      try {
        FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testFileClearing() throws Exception
  {
    String fileName = "s3-compaction_1.0";
    String path = testMeta.outputPath + Path.SEPARATOR + fileName;
    long size = 80;

    File file = new File(path);
    File tmpFile = new File(path + "." + System.currentTimeMillis() + ".tmp");
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < 10; i++) {
      sb.append("Record" + i + "\n");
      if (i == 5) {
        FileUtils.write(tmpFile, sb.toString());
      }
    }
    FileUtils.write(file, sb.toString());

    FSRecordCompactionOperator.OutputMetaData outputMetaData = new FSRecordCompactionOperator.OutputMetaData(path, fileName, size);
    testMeta.underTest.beginWindow(0);
    testMeta.underTest.input.process(outputMetaData);
    testMeta.underTest.endWindow();

    for (int i = 1; i < 60; i++) {
      testMeta.underTest.beginWindow(i);
      testMeta.underTest.endWindow();
    }
    testMeta.underTest.committed(59);
    for (int i = 60; i < 70; i++) {
      testMeta.underTest.beginWindow(i);
      Thread.sleep(10);
      testMeta.underTest.endWindow();
    }
    Collection<File> files =
        FileUtils.listFiles(new File(testMeta.outputPath), null, true);
    Assert.assertEquals(0, files.size());
  }
}

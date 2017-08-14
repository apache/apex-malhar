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
package org.apache.apex.malhar.lib.db.redshift;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator;
import org.apache.commons.io.FileUtils;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.mockito.Mockito.when;

public class RedshiftJdbcTransactionalOperatorTest
{
  private String inputDir;
  private static final String FILE_1 = "file1.txt";
  private static final String FILE_2 = "file2.txt";
  private static final String FILE_1_DATA = "460|xkalk|665\n950|xkalk|152\n850|xsblk|252";
  private static final String FILE_2_DATA = "640|xkalk|655\n50|bcklk|52";
  private static FSRecordCompactionOperator.OutputMetaData file1Meta;
  private static FSRecordCompactionOperator.OutputMetaData file2Meta;
  private static List<FSRecordCompactionOperator.OutputMetaData> listOfFiles = new ArrayList<>();
  private static List<String> data = new ArrayList<>();

  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;
    Context.OperatorContext context;
    @Mock
    public Statement statement;
    @Mock
    public JdbcTransactionalStore store;
    @Mock
    public Connection conn;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();

      MockitoAnnotations.initMocks(this);

      try {
        when(store.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(statement);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File(baseDirectory));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Before
  public void setup() throws Exception
  {
    inputDir = testMeta.baseDirectory + File.separator + "input";

    File file1 = new File(inputDir + File.separator + FILE_1);
    file1Meta = new FSRecordCompactionOperator.OutputMetaData(file1.getPath(), file1.getName(), file1.length());
    FileUtils.writeStringToFile(file1, FILE_1_DATA);

    File file2 = new File(inputDir + File.separator + FILE_2);
    file2Meta = new FSRecordCompactionOperator.OutputMetaData(file2.getPath(), file2.getName(), file2.length());
    FileUtils.writeStringToFile(file2, FILE_2_DATA);
  }

  @Test
  public void TestBatchData() throws SQLException, IOException
  {
    RedshiftJdbcTransactionableTestOutputOperator operator = new RedshiftJdbcTransactionableTestOutputOperator();
    operator.setReaderMode("READ_FROM_S3");
    operator.setStore(testMeta.store);
    operator.setAccessKey("accessKey");
    operator.setSecretKey("secretKey");
    operator.setBucketName("bucketName");

    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(Context.OperatorContext.SPIN_MILLIS, 500);
    attributeMap.put(Context.DAGContext.APPLICATION_PATH, testMeta.baseDirectory);
    testMeta.context = mockOperatorContext(1, attributeMap);;

    operator.setup(testMeta.context);
    operator.beginWindow(1);
    operator.input.process(file1Meta);
    operator.input.process(file2Meta);
    when(testMeta.statement.executeBatch()).thenReturn(executeBatch());
    operator.endWindow();
    Assert.assertEquals("Number of tuples in database", 5, data.size());
  }

  public int[] executeBatch() throws IOException
  {
    for (FSRecordCompactionOperator.OutputMetaData metaData: listOfFiles) {
      data.addAll(FileUtils.readLines(new File(metaData.getPath())));
    }
    return null;
  }

  @Test
  public void VerifyS3Properties()
  {
    RedshiftJdbcTransactionableTestOutputOperator operator = new RedshiftJdbcTransactionableTestOutputOperator();
    operator.setReaderMode("READ_FROM_S3");
    operator.setAccessKey("accessKey");
    operator.setSecretKey("secretKey");
    operator.setBucketName("bucketName");

    Assert.assertNotNull(operator.getBucketName());
  }

  @Test
  public void VerifyEMRProperties()
  {
    RedshiftJdbcTransactionableTestOutputOperator operator = new RedshiftJdbcTransactionableTestOutputOperator();
    operator.setReaderMode("READ_FROM_EMR");
    operator.setAccessKey("accessKey");
    operator.setSecretKey("secretKey");
    operator.setEmrClusterId("emrClusterId");

    Assert.assertNotNull(operator.getEmrClusterId());
  }

  public static class RedshiftJdbcTransactionableTestOutputOperator extends RedshiftJdbcTransactionableOutputOperator
  {
    @Override
    public void processTuple(FSRecordCompactionOperator.OutputMetaData tuple)
    {
      super.processTuple(tuple);
      listOfFiles.add(tuple);
    }
  }
}

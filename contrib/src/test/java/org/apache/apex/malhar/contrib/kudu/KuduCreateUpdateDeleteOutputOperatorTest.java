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
package org.apache.apex.malhar.contrib.kudu;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.helper.TestPortContext;
import static com.datatorrent.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.junit.Assert.assertEquals;


public class KuduCreateUpdateDeleteOutputOperatorTest
{


  private static final transient Logger LOG = LoggerFactory.getLogger(KuduCreateUpdateDeleteOutputOperatorTest.class);

  private static final String tableName = "unittests";

  private final String APP_ID = "TestKuduOutputOperator";

  private final int OPERATOR_ID_FOR_KUDU_CRUD = 0;

  private static KuduClient kuduClient;

  private static KuduTable kuduTable;

  private static Map<String,ColumnSchema> columnDefs = new HashMap<>();

  private BaseKuduOutputOperator simpleKuduOutputOperator;

  private OperatorContext contextForKuduOutputOperator;

  private TestPortContext testPortContextForKuduOutput;

  @BeforeClass
  public static void setup() throws Exception
  {
    kuduClient = getClientHandle();
    if (kuduClient.tableExists(tableName)) {
      kuduClient.deleteTable(tableName);
    }
    createTestTable(tableName,kuduClient);
    kuduTable = kuduClient.openTable(tableName);
  }

  @AfterClass
  public static void shutdown() throws Exception
  {
    kuduClient.close();
  }

  @Before
  public void setUpKuduOutputOperatorContext() throws Exception
  {
    Attribute.AttributeMap.DefaultAttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    contextForKuduOutputOperator = mockOperatorContext(OPERATOR_ID_FOR_KUDU_CRUD, attributeMap);
    simpleKuduOutputOperator = new BaseKuduOutputOperator();
    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, UnitTestTablePojo.class);
    testPortContextForKuduOutput = new TestPortContext(portAttributes);
    simpleKuduOutputOperator.setup(contextForKuduOutputOperator);
    simpleKuduOutputOperator.activate(contextForKuduOutputOperator);
    simpleKuduOutputOperator.input.setup(testPortContextForKuduOutput);
  }

  private static KuduClient getClientHandle() throws Exception
  {
    KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder("localhost:7051");
    KuduClient client = builder.build();
    return client;
  }

  private static void createTestTable(String tableName, KuduClient client) throws Exception
  {
    List<ColumnSchema> columns = new ArrayList<>();
    ColumnSchema intRowKeyCol = new ColumnSchema.ColumnSchemaBuilder("introwkey", Type.INT32)
        .key(true)
        .build();
    columns.add(intRowKeyCol);
    columnDefs.put("introwkey",intRowKeyCol);
    ColumnSchema stringRowKeyCol = new ColumnSchema.ColumnSchemaBuilder("stringrowkey", Type.STRING)
        .key(true)
        .build();
    columns.add(stringRowKeyCol);
    columnDefs.put("stringrowkey",stringRowKeyCol);
    ColumnSchema timestampRowKey = new ColumnSchema.ColumnSchemaBuilder("timestamprowkey", Type.UNIXTIME_MICROS)
        .key(true)
        .build();
    columns.add(timestampRowKey);
    columnDefs.put("timestamprowkey",timestampRowKey);
    ColumnSchema longData = new ColumnSchema.ColumnSchemaBuilder("longdata", Type.INT64)
        .build();
    columns.add(longData);
    columnDefs.put("longdata",longData);
    ColumnSchema stringData = new ColumnSchema.ColumnSchemaBuilder("stringdata", Type.STRING)
        .build();
    columns.add(stringData);
    columnDefs.put("stringdata",stringData);
    ColumnSchema timestampdata = new ColumnSchema.ColumnSchemaBuilder("timestampdata", Type.UNIXTIME_MICROS)
        .build();
    columns.add(timestampdata);
    columnDefs.put("timestampdata",timestampdata);
    ColumnSchema binarydata = new ColumnSchema.ColumnSchemaBuilder("binarydata", Type.BINARY)
        .build();
    columns.add(binarydata);
    columnDefs.put("binarydata",binarydata);
    ColumnSchema floatdata = new ColumnSchema.ColumnSchemaBuilder("floatdata", Type.FLOAT)
        .build();
    columns.add(floatdata);
    columnDefs.put("floatdata",floatdata);
    ColumnSchema booldata = new ColumnSchema.ColumnSchemaBuilder("booldata", Type.BOOL)
        .build();
    columns.add(booldata);
    columnDefs.put("booldata",booldata);
    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("stringrowkey");
    rangeKeys.add("timestamprowkey");
    List<String> hashPartitions = new ArrayList<>();
    hashPartitions.add("introwkey");
    Schema schema = new Schema(columns);
    try {
      client.createTable(tableName, schema,
          new CreateTableOptions()
          .setNumReplicas(1)
          .setRangePartitionColumns(rangeKeys)
          .addHashPartitions(hashPartitions,2));
    } catch (KuduException e) {
      LOG.error("Error while creating table for unit tests " + e.getMessage(), e);
      throw e;
    }
  }

  private void lookUpAndPopulateRecord(UnitTestTablePojo keyInfo) throws Exception
  {
    KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable)
        .addPredicate(KuduPredicate.newComparisonPredicate(columnDefs.get("introwkey"),
        KuduPredicate.ComparisonOp.EQUAL,keyInfo.getIntrowkey()))
        .addPredicate(KuduPredicate.newComparisonPredicate(columnDefs.get("stringrowkey"),
        KuduPredicate.ComparisonOp.EQUAL,keyInfo.getStringrowkey()))
        .addPredicate(KuduPredicate.newComparisonPredicate(columnDefs.get("timestamprowkey"),
        KuduPredicate.ComparisonOp.EQUAL,keyInfo.getTimestamprowkey()))
        .build();
    RowResultIterator rowResultItr = scanner.nextRows();
    while (rowResultItr.hasNext()) {
      RowResult thisRow = rowResultItr.next();
      keyInfo.setFloatdata(thisRow.getFloat("floatdata"));
      keyInfo.setBooldata(thisRow.getBoolean("booldata"));
      keyInfo.setBinarydata(thisRow.getBinary("binarydata"));
      keyInfo.setLongdata(thisRow.getLong("longdata"));
      keyInfo.setTimestampdata(thisRow.getLong("timestampdata"));
      keyInfo.setStringdata("stringdata");
      break;
    }
  }

  @Test
  public void processForUpdate() throws Exception
  {
    KuduExecutionContext<UnitTestTablePojo> newInsertExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojo = new UnitTestTablePojo();
    unitTestTablePojo.setIntrowkey(2);
    unitTestTablePojo.setStringrowkey("two" + System.currentTimeMillis());
    unitTestTablePojo.setTimestamprowkey(System.currentTimeMillis());
    unitTestTablePojo.setBooldata(true);
    unitTestTablePojo.setFloatdata(3.2f);
    unitTestTablePojo.setStringdata("" + System.currentTimeMillis());
    unitTestTablePojo.setLongdata(System.currentTimeMillis() + 1);
    unitTestTablePojo.setTimestampdata(System.currentTimeMillis() + 2);
    unitTestTablePojo.setBinarydata(ByteBuffer.wrap("stringdata".getBytes()));
    newInsertExecutionContext.setMutationType(KuduMutationType.INSERT);
    newInsertExecutionContext.setPayload(unitTestTablePojo);

    simpleKuduOutputOperator.beginWindow(1);
    simpleKuduOutputOperator.input.process(newInsertExecutionContext);
    KuduExecutionContext<UnitTestTablePojo> updateExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo updatingRecord = new UnitTestTablePojo();
    updateExecutionContext.setMutationType(KuduMutationType.UPDATE);
    updatingRecord.setBooldata(false);
    updatingRecord.setIntrowkey(unitTestTablePojo.getIntrowkey());
    updatingRecord.setStringrowkey(unitTestTablePojo.getStringrowkey());
    updatingRecord.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    updateExecutionContext.setPayload(updatingRecord);
    simpleKuduOutputOperator.input.process(updateExecutionContext);
    simpleKuduOutputOperator.endWindow();

    UnitTestTablePojo unitTestTablePojoRead = new UnitTestTablePojo();
    unitTestTablePojoRead.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoRead.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoRead.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    lookUpAndPopulateRecord(unitTestTablePojoRead);
    assertEquals(unitTestTablePojoRead.isBooldata(), false);
  }

  @Test
  public void processForUpsert() throws Exception
  {
    KuduExecutionContext<UnitTestTablePojo> upsertExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojo = new UnitTestTablePojo();
    unitTestTablePojo.setIntrowkey(3);
    unitTestTablePojo.setStringrowkey("three" + System.currentTimeMillis());
    unitTestTablePojo.setTimestamprowkey(System.currentTimeMillis());
    unitTestTablePojo.setBooldata(false);
    unitTestTablePojo.setFloatdata(3.2f);
    unitTestTablePojo.setStringdata("" + System.currentTimeMillis());
    unitTestTablePojo.setLongdata(System.currentTimeMillis() + 1);
    unitTestTablePojo.setTimestampdata(System.currentTimeMillis() + 2);
    unitTestTablePojo.setBinarydata(ByteBuffer.wrap("stringdata".getBytes()));
    upsertExecutionContext.setMutationType(KuduMutationType.UPSERT);
    upsertExecutionContext.setPayload(unitTestTablePojo);

    simpleKuduOutputOperator.beginWindow(2);
    simpleKuduOutputOperator.input.process(upsertExecutionContext);
    upsertExecutionContext.setMutationType(KuduMutationType.UPSERT);
    unitTestTablePojo.setBooldata(true);
    simpleKuduOutputOperator.input.process(upsertExecutionContext);
    simpleKuduOutputOperator.endWindow();

    UnitTestTablePojo unitTestTablePojoRead = new UnitTestTablePojo();
    unitTestTablePojoRead.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoRead.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoRead.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    lookUpAndPopulateRecord(unitTestTablePojoRead);
    assertEquals(unitTestTablePojoRead.isBooldata(), true);
  }

  @Test
  public void processForDelete() throws Exception
  {
    KuduExecutionContext<UnitTestTablePojo> insertExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojo = new UnitTestTablePojo();
    unitTestTablePojo.setIntrowkey(4);
    unitTestTablePojo.setStringrowkey("four" + System.currentTimeMillis());
    unitTestTablePojo.setTimestamprowkey(System.currentTimeMillis());
    unitTestTablePojo.setBooldata(false);
    unitTestTablePojo.setFloatdata(3.2f);
    unitTestTablePojo.setStringdata("" + System.currentTimeMillis());
    unitTestTablePojo.setLongdata(System.currentTimeMillis() + 1);
    unitTestTablePojo.setTimestampdata(System.currentTimeMillis() + 2);
    unitTestTablePojo.setBinarydata(ByteBuffer.wrap("stringdata".getBytes()));
    insertExecutionContext.setMutationType(KuduMutationType.INSERT);
    insertExecutionContext.setPayload(unitTestTablePojo);

    simpleKuduOutputOperator.beginWindow(3);
    simpleKuduOutputOperator.input.process(insertExecutionContext);
    KuduExecutionContext<UnitTestTablePojo> deleteExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojoDelete = new UnitTestTablePojo();
    unitTestTablePojoDelete.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoDelete.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoDelete.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    deleteExecutionContext.setMutationType(KuduMutationType.DELETE);
    deleteExecutionContext.setPayload(unitTestTablePojoDelete);
    simpleKuduOutputOperator.input.process(deleteExecutionContext);
    simpleKuduOutputOperator.endWindow();

    UnitTestTablePojo unitTestTablePojoRead = new UnitTestTablePojo();
    unitTestTablePojoRead.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoRead.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoRead.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    lookUpAndPopulateRecord(unitTestTablePojoRead);
    assertEquals(unitTestTablePojoRead.getBinarydata(), null);
  }

  @Test
  public void processForInsert() throws Exception
  {
    KuduExecutionContext<UnitTestTablePojo> insertExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojo = new UnitTestTablePojo();
    unitTestTablePojo.setIntrowkey(1);
    unitTestTablePojo.setStringrowkey("one" + System.currentTimeMillis());
    unitTestTablePojo.setTimestamprowkey(System.currentTimeMillis());
    unitTestTablePojo.setBooldata(true);
    unitTestTablePojo.setFloatdata(3.2f);
    unitTestTablePojo.setStringdata("" + System.currentTimeMillis());
    unitTestTablePojo.setLongdata(System.currentTimeMillis() + 1);
    unitTestTablePojo.setTimestampdata(System.currentTimeMillis() + 2);
    unitTestTablePojo.setBinarydata(ByteBuffer.wrap("stringdata".getBytes()));
    insertExecutionContext.setMutationType(KuduMutationType.INSERT);
    insertExecutionContext.setPayload(unitTestTablePojo);

    simpleKuduOutputOperator.beginWindow(0);
    simpleKuduOutputOperator.input.process(insertExecutionContext);
    simpleKuduOutputOperator.endWindow();

    UnitTestTablePojo unitTestTablePojoRead = new UnitTestTablePojo();
    unitTestTablePojoRead.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoRead.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoRead.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    lookUpAndPopulateRecord(unitTestTablePojoRead);
    assertEquals("" + unitTestTablePojoRead.getFloatdata(),"" + unitTestTablePojo.getFloatdata());
  }

}

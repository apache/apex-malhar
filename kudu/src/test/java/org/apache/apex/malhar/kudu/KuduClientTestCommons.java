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
package org.apache.apex.malhar.kudu;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import static org.powermock.api.mockito.PowerMockito.when;

/** Common test code for all kudu operators
 */
public class KuduClientTestCommons
{

  private static final transient Logger LOG = LoggerFactory.getLogger(KuduClientTestCommons.class);


  protected static final String tableName = "unittests";

  protected static KuduClient kuduClient;

  protected static KuduTable kuduTable;

  protected static Schema schemaForUnitTests;

  protected static String kuduMasterAddresses = "192.168.1.41:7051";

  public static final int SPLIT_COUNT_FOR_INT_ROW_KEY = 5;

  public static final int HASH_BUCKETS_SIZE_FOR_ALL_HASH_COL = 2;

  public static final int TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE = 12;

  public static boolean tableInitialized = false;

  public static Object objectForLocking = new Object();

  protected static Map<String,ColumnSchema> columnDefs = new HashMap<>();


  public static void setup() throws Exception
  {
    kuduClient = getClientHandle();
    if (kuduClient.tableExists(tableName)) {
      kuduClient.deleteTable(tableName);
    }
    createTestTable(tableName,kuduClient);
    kuduTable = kuduClient.openTable(tableName);
    tableInitialized = true;
  }


  public static void shutdown() throws Exception
  {
    kuduClient.close();
  }


  private static KuduClient getClientHandle() throws Exception
  {
    KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(kuduMasterAddresses);
    KuduClient client = builder.build();
    return client;
  }

  public static ApexKuduConnection.ApexKuduConnectionBuilder getConnectionConfigForTable()
  {
    ApexKuduConnection.ApexKuduConnectionBuilder connectionBuilder = new ApexKuduConnection.ApexKuduConnectionBuilder();
    return connectionBuilder.withAPossibleMasterHostAs(kuduMasterAddresses).withTableName(tableName);
  }

  public static Schema buildSchemaForUnitTestsTable() throws Exception
  {
    if (schemaForUnitTests != null) {
      return schemaForUnitTests;
    }
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
        .nullable(true)
        .build();
    columns.add(longData);
    columnDefs.put("longdata",longData);
    ColumnSchema stringData = new ColumnSchema.ColumnSchemaBuilder("stringdata", Type.STRING)
        .nullable(true)
        .build();
    columns.add(stringData);
    columnDefs.put("stringdata",stringData);
    ColumnSchema timestampdata = new ColumnSchema.ColumnSchemaBuilder("timestampdata", Type.UNIXTIME_MICROS)
        .nullable(true)
        .build();
    columns.add(timestampdata);
    columnDefs.put("timestampdata",timestampdata);
    ColumnSchema binarydata = new ColumnSchema.ColumnSchemaBuilder("binarydata", Type.BINARY)
        .nullable(true)
        .build();
    columns.add(binarydata);
    columnDefs.put("binarydata",binarydata);
    ColumnSchema floatdata = new ColumnSchema.ColumnSchemaBuilder("floatdata", Type.FLOAT)
        .nullable(true)
        .build();
    columns.add(floatdata);
    columnDefs.put("floatdata",floatdata);
    ColumnSchema booldata = new ColumnSchema.ColumnSchemaBuilder("booldata", Type.BOOL)
        .nullable(true)
        .build();
    columns.add(booldata);
    columnDefs.put("booldata",booldata);
    schemaForUnitTests = new Schema(columns);
    return schemaForUnitTests;
  }

  public static void createTestTable(String tableName, KuduClient client) throws Exception
  {
    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("introwkey");
    List<String> hashPartitions = new ArrayList<>();
    hashPartitions.add("stringrowkey");
    hashPartitions.add("timestamprowkey");
    CreateTableOptions thisTableOptions = new CreateTableOptions()
        .setNumReplicas(1)
        .addHashPartitions(hashPartitions,HASH_BUCKETS_SIZE_FOR_ALL_HASH_COL)
        .setRangePartitionColumns(rangeKeys);
    int stepsize = Integer.MAX_VALUE / SPLIT_COUNT_FOR_INT_ROW_KEY;
    int splitBoundary = stepsize;
    Schema schema = buildSchemaForUnitTestsTable();
    for ( int i = 0; i < SPLIT_COUNT_FOR_INT_ROW_KEY; i++) {
      PartialRow splitRowBoundary = schema.newPartialRow();
      splitRowBoundary.addInt("introwkey",splitBoundary);
      thisTableOptions = thisTableOptions.addSplitRow(splitRowBoundary);
      splitBoundary += stepsize;
    }
    try {
      client.createTable(tableName, schema,thisTableOptions);
    } catch (KuduException e) {
      LOG.error("Error while creating table for unit tests " + e.getMessage(), e);
      throw e;
    }

  }

  protected void lookUpAndPopulateRecord(UnitTestTablePojo keyInfo) throws Exception
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

  public ApexKuduConnection buildMockWiring(AbstractKuduInputOperator abstractKuduInputOperator,
      int numScanTokens) throws Exception
  {
    ApexKuduConnection mockedConnectionHandle = PowerMockito.mock(ApexKuduConnection.class);
    ApexKuduConnection.ApexKuduConnectionBuilder mockedConnectionHandleBuilder = PowerMockito.mock(
        ApexKuduConnection.ApexKuduConnectionBuilder.class);
    KuduClient mockedClient = PowerMockito.mock(KuduClient.class);
    KuduSession mockedKuduSession = PowerMockito.mock(KuduSession.class);
    KuduTable mockedKuduTable = PowerMockito.mock(KuduTable.class);
    KuduScanToken.KuduScanTokenBuilder mockedScanTokenBuilder = PowerMockito.mock(
        KuduScanToken.KuduScanTokenBuilder.class);
    List<KuduScanToken> mockedScanTokens = new ArrayList<>();
    int scanTokensToBuild = numScanTokens;
    for (int i = 0; i < scanTokensToBuild; i++) {
      mockedScanTokens.add(PowerMockito.mock(KuduScanToken.class));
    }
    PowerMockito.mockStatic(KryoCloneUtils.class);
    when(KryoCloneUtils.cloneObject(abstractKuduInputOperator)).thenReturn(abstractKuduInputOperator);
    //wire the mocks
    when(abstractKuduInputOperator.getApexKuduConnectionInfo()).thenReturn(mockedConnectionHandleBuilder);
    when(mockedConnectionHandle.getKuduClient()).thenReturn(mockedClient);
    when(mockedClient.newSession()).thenReturn(mockedKuduSession);
    when(mockedConnectionHandle.getKuduTable()).thenReturn(mockedKuduTable);
    when(mockedConnectionHandle.getKuduSession()).thenReturn(mockedKuduSession);
    when(mockedConnectionHandle.getBuilderForThisConnection()).thenReturn(mockedConnectionHandleBuilder);
    when(mockedClient.openTable(tableName)).thenReturn(mockedKuduTable);
    when(mockedConnectionHandleBuilder.build()).thenReturn(mockedConnectionHandle);
    when(mockedKuduTable.getSchema()).thenReturn(schemaForUnitTests);
    when(mockedClient.newScanTokenBuilder(mockedKuduTable)).thenReturn(mockedScanTokenBuilder);
    when(mockedScanTokenBuilder.build()).thenReturn(mockedScanTokens);
    return mockedConnectionHandle;
  }

  public static String getKuduMasterAddresses()
  {
    return kuduMasterAddresses;
  }

  public static void setKuduMasterAddresses(String kuduMasterAddresses)
  {
    KuduClientTestCommons.kuduMasterAddresses = kuduMasterAddresses;
  }
}

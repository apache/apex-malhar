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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.partitioner.KuduPartitionScanStrategy;
import org.apache.apex.malhar.kudu.scanner.AbstractKuduPartitionScanner;
import org.apache.apex.malhar.kudu.scanner.KuduScanOrderStrategy;
import org.apache.apex.malhar.lib.helper.TestPortContext;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.Upsert;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class KuduInputOperatorCommons extends KuduClientTestCommons
{
  private static final Logger LOG = LoggerFactory.getLogger(KuduInputOperatorCommons.class);

  public static final String APP_ID = "TestKuduInputOperatorOneToOnePartitioner";
  public static final int OPERATOR_ID_FOR_ONE_TO_ONE_PARTITIONER = 1;

  protected Context.OperatorContext operatorContext;
  protected TestPortContext testPortContext;
  protected UnitTestStepwiseScanInputOperator unitTestStepwiseScanInputOperator;
  protected Collection<Partitioner.Partition<AbstractKuduInputOperator>> partitions;

  protected  KuduPartitionScanStrategy partitonScanStrategy = KuduPartitionScanStrategy.ONE_TABLET_PER_OPERATOR;

  protected  KuduScanOrderStrategy scanOrderStrategy = KuduScanOrderStrategy.RANDOM_ORDER_SCANNER;

  protected static int numberOfKuduInputOperatorPartitions = 5;

  protected static Partitioner.PartitioningContext partitioningContext;

  public void initOperatorState() throws Exception
  {
    Attribute.AttributeMap.DefaultAttributeMap attributeMapForInputOperator =
        new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMapForInputOperator.put(DAG.APPLICATION_ID, APP_ID);
    operatorContext = mockOperatorContext(OPERATOR_ID_FOR_ONE_TO_ONE_PARTITIONER,
      attributeMapForInputOperator);

    Attribute.AttributeMap.DefaultAttributeMap portAttributesForInputOperator =
        new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributesForInputOperator.put(Context.PortContext.TUPLE_CLASS, UnitTestTablePojo.class);
    testPortContext = new TestPortContext(portAttributesForInputOperator);

    unitTestStepwiseScanInputOperator = new UnitTestStepwiseScanInputOperator(
        getConnectionConfigForTable(), UnitTestTablePojo.class);
    unitTestStepwiseScanInputOperator.setNumberOfPartitions(numberOfKuduInputOperatorPartitions);
    unitTestStepwiseScanInputOperator.setPartitionScanStrategy(partitonScanStrategy);
    unitTestStepwiseScanInputOperator.setScanOrderStrategy(scanOrderStrategy);
    initCommonConfigsForAllTypesOfTests();
    partitions = unitTestStepwiseScanInputOperator.definePartitions(
      new ArrayList(), partitioningContext);
    Iterator<Partitioner.Partition<AbstractKuduInputOperator>> iteratorForMeta = partitions.iterator();
    UnitTestStepwiseScanInputOperator actualOperator =
        (UnitTestStepwiseScanInputOperator)iteratorForMeta.next()
        .getPartitionedInstance();
    // Adjust the bindings as if apex has completed the partioning.The runtime of the framework does this in reality
    unitTestStepwiseScanInputOperator = actualOperator;
    unitTestStepwiseScanInputOperator.setup(operatorContext);
    unitTestStepwiseScanInputOperator.activate(operatorContext);
    //rewire parent operator to enable proper unit testing method calls
    unitTestStepwiseScanInputOperator.getPartitioner().setPrototypeKuduInputOperator(unitTestStepwiseScanInputOperator);
    unitTestStepwiseScanInputOperator.getScanner().setParentOperator(unitTestStepwiseScanInputOperator);
  }

  public static void initCommonConfigsForAllTypesOfTests() throws Exception
  {
    KuduClientTestCommons.buildSchemaForUnitTestsTable();
    partitioningContext = new Partitioner.PartitioningContext()
    {
      @Override
      public int getParallelPartitionCount()
      {
        return numberOfKuduInputOperatorPartitions;
      }

      @Override
      public List<Operator.InputPort<?>> getInputPorts()
      {
        return null;
      }
    };
  }

  public void truncateTable() throws Exception
  {
    AbstractKuduPartitionScanner<UnitTestTablePojo,InputOperatorControlTuple> scannerForDeletingRows =
        unitTestStepwiseScanInputOperator.getScanner();
    List<KuduScanToken> scansForAllTablets = unitTestStepwiseScanInputOperator
        .getPartitioner().getKuduScanTokensForSelectAllColumns();
    ApexKuduConnection aCurrentConnection = scannerForDeletingRows.getConnectionPoolForThreads().get(0);
    KuduSession aSessionForDeletes = aCurrentConnection.getKuduClient().newSession();
    KuduTable currentTable = aCurrentConnection.getKuduTable();
    for ( KuduScanToken aTabletScanToken : scansForAllTablets) {
      KuduScanner aScanner = aTabletScanToken.intoScanner(aCurrentConnection.getKuduClient());
      while ( aScanner.hasMoreRows()) {
        RowResultIterator itrForRows = aScanner.nextRows();
        while ( itrForRows.hasNext()) {
          RowResult aRow = itrForRows.next();
          int intRowKey = aRow.getInt("introwkey");
          String stringRowKey = aRow.getString("stringrowkey");
          long timestampRowKey = aRow.getLong("timestamprowkey");
          Delete aDeleteOp = currentTable.newDelete();
          aDeleteOp.getRow().addInt("introwkey",intRowKey);
          aDeleteOp.getRow().addString("stringrowkey", stringRowKey);
          aDeleteOp.getRow().addLong("timestamprowkey",timestampRowKey);
          aSessionForDeletes.apply(aDeleteOp);
        }
      }
    }
    aSessionForDeletes.close();
    Thread.sleep(2000); // Sleep to allow for scans to complete
  }

  public void addTestDataRows(int numRowsInEachPartition) throws Exception
  {
    int intRowKeyStepsize = Integer.MAX_VALUE / SPLIT_COUNT_FOR_INT_ROW_KEY;
    int splitBoundaryForIntRowKey = intRowKeyStepsize;
    int[] inputrowkeyPartitionEntries = new int[SPLIT_COUNT_FOR_INT_ROW_KEY + 1];
    // setting the int keys that will fall in the range of all partitions
    for ( int i = 0; i < SPLIT_COUNT_FOR_INT_ROW_KEY; i++) {
      inputrowkeyPartitionEntries[i] = splitBoundaryForIntRowKey + 3; // 3 to fall into the partition next to boundary
      splitBoundaryForIntRowKey += intRowKeyStepsize;
    }
    inputrowkeyPartitionEntries[SPLIT_COUNT_FOR_INT_ROW_KEY] = splitBoundaryForIntRowKey + 3;
    AbstractKuduPartitionScanner<UnitTestTablePojo,InputOperatorControlTuple> scannerForAddingRows =
        unitTestStepwiseScanInputOperator.getScanner();
    ApexKuduConnection aCurrentConnection = scannerForAddingRows.getConnectionPoolForThreads().get(0);
    KuduSession aSessionForInserts = aCurrentConnection.getKuduClient().newSession();
    KuduTable currentTable = aCurrentConnection.getKuduTable();
    long seedValueForTimestampRowKey = 0L; // constant to allow for data landing on first partition for unit tests
    for ( int i = 0; i <= SPLIT_COUNT_FOR_INT_ROW_KEY; i++) { // range key iterator
      int intRowKeyBaseValue = inputrowkeyPartitionEntries[i] + i;
      for ( int k = 0; k < 2; k++) { // hash key iterator . The table defines two hash partitions
        long timestampRowKeyValue = seedValueForTimestampRowKey + k; // to avoid spilling to another tablet
        String stringRowKeyValue = "" + timestampRowKeyValue + k; // to avoid spilling to another tablet randomly
        for ( int y = 0; y < numRowsInEachPartition; y++) {
          Upsert aNewRow = currentTable.newUpsert();
          PartialRow rowValue  = aNewRow.getRow();
          // Start assigning row keys below the current split boundary.
          rowValue.addInt("introwkey",intRowKeyBaseValue - y - 1);
          rowValue.addString("stringrowkey",stringRowKeyValue);
          rowValue.addLong("timestamprowkey",timestampRowKeyValue);
          rowValue.addLong("longdata",(seedValueForTimestampRowKey + y));
          rowValue.addString("stringdata", ("" + seedValueForTimestampRowKey + y));
          OperationResponse response = aSessionForInserts.apply(aNewRow);
        }
      }
    }
    List<OperationResponse> insertResponse = aSessionForInserts.flush();
    aSessionForInserts.close();
    Thread.sleep(2000); // Sleep to allow for scans to complete
  }

  public long countNumRowsInTable() throws Exception
  {
    List<String> allProjectedCols = new ArrayList<>(
        unitTestStepwiseScanInputOperator.getKuduColNameToSchemaMapping().keySet());
    KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable)
        .setProjectedColumnNames(allProjectedCols)
        .build();
    long counter = 0;
    while (scanner.hasMoreRows()) {
      RowResultIterator rowResultItr = scanner.nextRows();
      while (rowResultItr.hasNext()) {
        RowResult thisRow = rowResultItr.next();
        counter += 1;
      }
    }
    return counter;
  }
}

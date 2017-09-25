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
package org.apache.apex.malhar.kudu.scanner;

import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.kudu.InputOperatorControlTuple;
import org.apache.apex.malhar.kudu.KuduClientTestCommons;
import org.apache.apex.malhar.kudu.KuduInputOperatorCommons;
import org.apache.apex.malhar.kudu.UnitTestTablePojo;
import org.apache.apex.malhar.kudu.partitioner.KuduPartitionScanStrategy;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;
import org.apache.apex.malhar.kudu.test.KuduClusterAvailabilityTestRule;
import org.apache.apex.malhar.kudu.test.KuduClusterTestContext;
import org.apache.kudu.ColumnSchema;

import static org.junit.Assert.assertEquals;

public class KuduPartitionScannerCallableTest extends KuduInputOperatorCommons
{
  @Rule
  public KuduClusterAvailabilityTestRule kuduClusterAvailabilityTestRule = new KuduClusterAvailabilityTestRule();

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testSettersForPojo() throws Exception
  {
    initOperatorState();
    AbstractKuduPartitionScanner<UnitTestTablePojo,InputOperatorControlTuple> currentScanner =
        unitTestStepwiseScanInputOperator.getScanner();
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        "select introwkey as intColumn from unittests",
        new ArrayList<ColumnSchema>(columnDefs.values()));
    List<KuduPartitionScanAssignmentMeta> scansForThisQuery = currentScanner.preparePlanForScanners(translator);
    KuduPartitionScannerCallable<UnitTestTablePojo,InputOperatorControlTuple> threadToScan = new
        KuduPartitionScannerCallable<>(unitTestStepwiseScanInputOperator,scansForThisQuery.get(0),
        currentScanner.getConnectionPoolForThreads().get(0),
        unitTestStepwiseScanInputOperator.extractSettersForResultObject(translator),translator);
    long countOfScan = threadToScan.call();
  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testRowScansForAllDataAcrossAllPartitions() throws Exception
  {
    partitonScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;
    numberOfKuduInputOperatorPartitions = 1;
    initOperatorState();
    AbstractKuduPartitionScanner<UnitTestTablePojo,InputOperatorControlTuple> currentScanner =
        unitTestStepwiseScanInputOperator.getScanner();
    // truncate and add some data to the unit test table
    truncateTable();
    addTestDataRows(10); // This is per partition and there are 12 partitions
    assertEquals((KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE * 10 ),countNumRowsInTable());
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        "select * from unittests",
        new ArrayList<ColumnSchema>(columnDefs.values()));
    List<KuduPartitionScanAssignmentMeta> scansForThisQuery = currentScanner.preparePlanForScanners(translator);
    // Now scan for exact match of counts
    long totalRowsRead = 0;
    unitTestStepwiseScanInputOperator.getBuffer().clear();
    for (KuduPartitionScanAssignmentMeta aSegmentToScan :  scansForThisQuery) {
      KuduPartitionScannerCallable<UnitTestTablePojo,InputOperatorControlTuple> threadToScan = new
          KuduPartitionScannerCallable<>(unitTestStepwiseScanInputOperator,aSegmentToScan,
          currentScanner.getConnectionPoolForThreads().get(0),
          unitTestStepwiseScanInputOperator.extractSettersForResultObject(translator),translator);
      totalRowsRead += threadToScan.call();
    }
    // 144 = 120 records + 12 * 2 markers
    int expectedCount = ( 10 * KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE) +
        ( 2 * KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE);
    assertEquals(expectedCount,unitTestStepwiseScanInputOperator.getBuffer().size());
    // revert all configs to default
    partitonScanStrategy = KuduPartitionScanStrategy.ONE_TABLET_PER_OPERATOR;
    numberOfKuduInputOperatorPartitions = 5;
  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testRowScansForAllDataInSinglePartition() throws Exception
  {
    partitonScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;
    initOperatorState();
    AbstractKuduPartitionScanner<UnitTestTablePojo,InputOperatorControlTuple> currentScanner =
        unitTestStepwiseScanInputOperator.getScanner();
    // truncate and add some data to the unit test table
    truncateTable();
    addTestDataRows(10); // This is per partition and there are 12 partitions
    assertEquals((10 * KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE),countNumRowsInTable());
    int intRowBoundary = Integer.MAX_VALUE / SPLIT_COUNT_FOR_INT_ROW_KEY; // 5 to allow ofr scan to fall in lower
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        "select * from unittests where introwkey < " + intRowBoundary,
        new ArrayList<ColumnSchema>(columnDefs.values()));
    List<KuduPartitionScanAssignmentMeta> scansForThisQuery = currentScanner.preparePlanForScanners(translator);
    // Now scan for exact match of counts
    long totalRowsRead = 0;
    unitTestStepwiseScanInputOperator.getBuffer().clear();
    for (KuduPartitionScanAssignmentMeta aSegmentToScan :  scansForThisQuery) {
      KuduPartitionScannerCallable<UnitTestTablePojo,InputOperatorControlTuple> threadToScan = new
          KuduPartitionScannerCallable<>(unitTestStepwiseScanInputOperator,aSegmentToScan,
          currentScanner.getConnectionPoolForThreads().get(0),
          unitTestStepwiseScanInputOperator.extractSettersForResultObject(translator),translator);
      totalRowsRead += threadToScan.call();
    }
    // 23  because of the hash distributions and the scan markers. 21 are data records and 2 are end of scan markers
    assertEquals(23L,unitTestStepwiseScanInputOperator.getBuffer().size());
    // revert all configs to default
    partitonScanStrategy = KuduPartitionScanStrategy.ONE_TABLET_PER_OPERATOR;
    numberOfKuduInputOperatorPartitions = 5;
  }


}

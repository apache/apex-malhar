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

import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.kudu.partitioner.KuduPartitionScanStrategy;
import org.apache.apex.malhar.kudu.scanner.AbstractKuduPartitionScanner;
import org.apache.apex.malhar.kudu.test.KuduClusterAvailabilityTestRule;
import org.apache.apex.malhar.kudu.test.KuduClusterTestContext;


import static org.junit.Assert.assertEquals;

public class AbstractKuduInputOperatorTest extends KuduInputOperatorCommons
{
  @Rule
  public KuduClusterAvailabilityTestRule kuduClusterAvailabilityTestRule = new KuduClusterAvailabilityTestRule();

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testForErrorPortInCaseOfSQLError() throws Exception
  {
    partitonScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;
    numberOfKuduInputOperatorPartitions = 5;
    initOperatorState();
    truncateTable();
    addTestDataRows(10);
    unitTestStepwiseScanInputOperator.getBuffer().clear();
    unitTestStepwiseScanInputOperator.beginWindow(2L);
    unitTestStepwiseScanInputOperator.processForQueryString("Select * from unittests where ");
    unitTestStepwiseScanInputOperator.endWindow();
    Thread.sleep(10000); // Sleep to allow for scans to complete
    assertEquals(0,unitTestStepwiseScanInputOperator.getBuffer().size());
    partitonScanStrategy = KuduPartitionScanStrategy.ONE_TABLET_PER_OPERATOR;
    numberOfKuduInputOperatorPartitions = 5;

  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testForSendingBeginAndEndScanMarkers() throws Exception
  {
    partitonScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;
    numberOfKuduInputOperatorPartitions = 1;
    initOperatorState();
    AbstractKuduPartitionScanner<UnitTestTablePojo,InputOperatorControlTuple> currentScanner =
        unitTestStepwiseScanInputOperator.getScanner();
    // truncate and add some data to the unit test table
    truncateTable();
    addTestDataRows(10); // This is per partition and there are 12 partitions
    unitTestStepwiseScanInputOperator.getBuffer().clear();
    unitTestStepwiseScanInputOperator.beginWindow(5);
    unitTestStepwiseScanInputOperator.processForQueryString("SELECT * FROM unittests ");
    Thread.sleep(10000); // Sleep to allow for scans to complete
    int exptectedCount = (10 * KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE) +
        ( 2 * KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE);
    assertEquals(exptectedCount,unitTestStepwiseScanInputOperator.getBuffer().size());
    // below only for debugging session usage and hence no asserts
    for ( int i = 0; i < exptectedCount; i++) { // 12 partitions : 144 = 12 * 10 + 12 * 2 ( begin , end scan markers )
      unitTestStepwiseScanInputOperator.emitTuples();
    }
    unitTestStepwiseScanInputOperator.endWindow();
    // revert all of the changes
    partitonScanStrategy = KuduPartitionScanStrategy.ONE_TABLET_PER_OPERATOR;
    numberOfKuduInputOperatorPartitions = 5;

  }
}

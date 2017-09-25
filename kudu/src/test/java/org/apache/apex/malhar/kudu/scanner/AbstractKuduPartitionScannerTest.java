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
import org.apache.apex.malhar.kudu.KuduInputOperatorCommons;
import org.apache.apex.malhar.kudu.UnitTestTablePojo;
import org.apache.apex.malhar.kudu.partitioner.KuduPartitionScanStrategy;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;
import org.apache.apex.malhar.kudu.test.KuduClusterAvailabilityTestRule;
import org.apache.apex.malhar.kudu.test.KuduClusterTestContext;
import org.apache.kudu.ColumnSchema;

import static org.junit.Assert.assertEquals;

public class AbstractKuduPartitionScannerTest extends KuduInputOperatorCommons
{
  @Rule
  public KuduClusterAvailabilityTestRule kuduClusterAvailabilityTestRule = new KuduClusterAvailabilityTestRule();

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void textPrepaprePlanForScanners() throws Exception
  {
    // no predicates test
    initOperatorState();
    AbstractKuduPartitionScanner<UnitTestTablePojo, InputOperatorControlTuple> scanner =
        unitTestStepwiseScanInputOperator.getScanner();
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        "select introwkey as intColumn from unittests",
        new ArrayList<ColumnSchema>(columnDefs.values()));
    List<KuduPartitionScanAssignmentMeta> scansForThisQuery = scanner.preparePlanForScanners(translator);
    // No predicates and hence a full scan and 1-1 implies one partition for this attempt
    assertEquals(1,scansForThisQuery.size());
    // many to one partitioner and no predicates
    numberOfKuduInputOperatorPartitions = 4;
    partitonScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;
    initOperatorState();
    translator = new SQLToKuduPredicatesTranslator(
      "select introwkey as intColumn from unittests",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    scanner = unitTestStepwiseScanInputOperator.getScanner();
    scansForThisQuery = scanner.preparePlanForScanners(translator);
    assertEquals(3,scansForThisQuery.size()); // 12 kudu partitions over 4 Apex partitions = 3
    // many to one partitioner and no predicates
    numberOfKuduInputOperatorPartitions = 4;
    partitonScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;
    initOperatorState();
    translator = new SQLToKuduPredicatesTranslator(
      "select introwkey as intColumn from unittests where introwkey = 1",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    scanner = unitTestStepwiseScanInputOperator.getScanner();
    scansForThisQuery = scanner.preparePlanForScanners(translator);
    // This query will actually result in two tablet scans as there is a hash partition involved as well apart from
    // range partitioning. However this operator is getting one scan as part of the pie assignment. Hence assert 1
    assertEquals(1,scansForThisQuery.size());

    //revert all changes back for subsequent tests
    numberOfKuduInputOperatorPartitions = 5;
    partitonScanStrategy = KuduPartitionScanStrategy.ONE_TABLET_PER_OPERATOR;

  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testForverifyConnectionStaleness() throws Exception
  {
    initOperatorState();
    AbstractKuduPartitionScanner<UnitTestTablePojo, InputOperatorControlTuple> scanner =
        unitTestStepwiseScanInputOperator.getScanner();
    scanner.connectionPoolForThreads.get(0).close(); // forcefully close
    assertEquals(true,scanner.connectionPoolForThreads.get(0).getKuduSession().isClosed());
    scanner.verifyConnectionStaleness(0);
    assertEquals(false,scanner.connectionPoolForThreads.get(0).getKuduSession().isClosed());
  }
}

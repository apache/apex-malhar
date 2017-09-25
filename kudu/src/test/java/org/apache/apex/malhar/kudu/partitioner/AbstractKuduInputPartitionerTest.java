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
package org.apache.apex.malhar.kudu.partitioner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.InputOperatorControlTuple;
import org.apache.apex.malhar.kudu.KuduClientTestCommons;
import org.apache.apex.malhar.kudu.KuduInputOperatorCommons;
import org.apache.apex.malhar.kudu.UnitTestTablePojo;
import org.apache.apex.malhar.kudu.scanner.KuduPartitionScanAssignmentMeta;
import org.apache.apex.malhar.kudu.test.KuduClusterAvailabilityTestRule;
import org.apache.apex.malhar.kudu.test.KuduClusterTestContext;
import org.apache.kudu.client.KuduScanToken;

import com.datatorrent.api.Partitioner;

import static org.junit.Assert.assertEquals;

public class AbstractKuduInputPartitionerTest extends KuduInputOperatorCommons
{
  @Rule
  public KuduClusterAvailabilityTestRule kuduClusterAvailabilityTestRule = new KuduClusterAvailabilityTestRule();

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testKuduSelectAllScanTokens() throws Exception
  {
    initOperatorState();
    AbstractKuduInputPartitioner partitioner = unitTestStepwiseScanInputOperator.getPartitioner();
    List<KuduScanToken> allScanTokens = partitioner.getKuduScanTokensForSelectAllColumns();
    assertEquals(KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE,allScanTokens.size());
  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testNumberOfPartitions() throws Exception
  {
    numberOfKuduInputOperatorPartitions = -1; // situation when the user has not set the partitions explicitly
    initOperatorState();
    AbstractKuduInputPartitioner partitioner = unitTestStepwiseScanInputOperator.getPartitioner();
    assertEquals(1,partitioner.getNumberOfPartitions(partitioningContext));
    numberOfKuduInputOperatorPartitions = 7; // situation when the user has not set the partitions explicitly
    initOperatorState();
    partitioner = unitTestStepwiseScanInputOperator.getPartitioner();
    assertEquals(7,partitioner.getNumberOfPartitions(partitioningContext));
    numberOfKuduInputOperatorPartitions = -1; // revert the value back to default
  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testDefinePartitions() throws Exception
  {
    // case of setting num partitions to 7 but setting one to one mapping.
    numberOfKuduInputOperatorPartitions = 7;
    initOperatorState();
    AbstractKuduInputPartitioner partitioner = unitTestStepwiseScanInputOperator.getPartitioner();
    assertEquals(KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE,partitioner.definePartitions(
        new ArrayList<Partitioner.Partition<AbstractKuduInputOperator>>(),partitioningContext).size());
    // case of setting num partition to 7 but go with many to one mapping
    partitonScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;
    initOperatorState();
    partitioner = unitTestStepwiseScanInputOperator.getPartitioner();
    Collection<Partitioner.Partition<AbstractKuduInputOperator>> partitionsCalculated = partitioner.definePartitions(
        new ArrayList<Partitioner.Partition<AbstractKuduInputOperator>>(),partitioningContext);
    assertEquals(7,partitionsCalculated.size());
    int maxPartitionPerOperator = -1;
    int minPartitionPerOperator = Integer.MAX_VALUE;
    Iterator<Partitioner.Partition<AbstractKuduInputOperator>> iteratorForPartitionScan =
        partitionsCalculated.iterator();
    while ( iteratorForPartitionScan.hasNext()) {
      Partitioner.Partition<AbstractKuduInputOperator> anOperatorPartition = iteratorForPartitionScan.next();
      AbstractKuduInputOperator<UnitTestTablePojo,InputOperatorControlTuple> anOperatorHandle =
          anOperatorPartition.getPartitionedInstance();
      List<KuduPartitionScanAssignmentMeta> partitionsAssigned = anOperatorHandle.getPartitionPieAssignment();
      if (  partitionsAssigned.size() > maxPartitionPerOperator ) {
        maxPartitionPerOperator = partitionsAssigned.size();
      }
      if ( partitionsAssigned.size() < minPartitionPerOperator) {
        minPartitionPerOperator = partitionsAssigned.size();
      }
    }
    assertEquals(2,maxPartitionPerOperator); // 7 kudu operator instances dealing with 12 kudu tablets
    assertEquals(1,minPartitionPerOperator); // 7 kudu operator instances dealing with 12 kudu tablets
    // revert all the changes to defaults for the next test
    numberOfKuduInputOperatorPartitions = 5;
    partitonScanStrategy = KuduPartitionScanStrategy.ONE_TABLET_PER_OPERATOR;
  }

}

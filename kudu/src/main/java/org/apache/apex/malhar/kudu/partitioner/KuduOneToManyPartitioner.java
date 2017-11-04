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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.scanner.AbstractKuduPartitionScanner;
import org.apache.apex.malhar.kudu.scanner.KuduPartitionScanAssignmentMeta;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;


/**
 * Used when a user would like to assign multiple kudu tablets to a single physical instance of the Kudu input operator
 *
 * @since 3.8.0
 */
public class KuduOneToManyPartitioner extends AbstractKuduInputPartitioner
{
  private static final Logger LOG = LoggerFactory.getLogger(KuduOneToManyPartitioner.class);

  public KuduOneToManyPartitioner(AbstractKuduInputOperator prototypeOperator)
  {
    super(prototypeOperator);
  }

  /**
   * Distributes the tablets evenly among the physical instances of the kudu input operator. Please see
   * {@link AbstractKuduPartitionScanner#preparePlanForScanners(SQLToKuduPredicatesTranslator)} for details as to how
   * a query based planning is done.
   * @param totalList The total list of possible tablet scans for all queries
   * @param context The context that is provided by the framework when repartitioning is to be executed
   * @return  Map with key as operator id and value as the a list of assignments that can be assigned to that operator.
   */
  @Override
  public Map<Integer, List<KuduPartitionScanAssignmentMeta>> assign(List<KuduPartitionScanAssignmentMeta> totalList,
      PartitioningContext context)
  {
    Map<Integer,List<KuduPartitionScanAssignmentMeta>> partitionAssignments = new HashMap<>();
    int partitionCount = getNumberOfPartitions(context);
    if ( partitionCount <= 0) {
      LOG.error(" Partition count cannot be zero ");
      partitionCount = 1; // set to a minimum of one.
    }
    int idealDistributionRatio = (totalList.size() / partitionCount) + 1;
    LOG.info(" Distributing not more than " + idealDistributionRatio + " partitions per input operator");
    int counterForLoopingTotal = 0;
    int totalSizeOfKuduScanAssignments = totalList.size();
    for ( int i = 0; i < partitionCount; i++) {
      partitionAssignments.put(i, new ArrayList<KuduPartitionScanAssignmentMeta>());
    }
    // We round robin all of the scan assignments so that all of the apex partitioned operators a part of the effort
    while ( counterForLoopingTotal < totalSizeOfKuduScanAssignments) {
      for (int i = 0; i < partitionCount; i++) {
        List<KuduPartitionScanAssignmentMeta> assignmentsForThisOperatorId = partitionAssignments.get(i);
        if (counterForLoopingTotal < totalSizeOfKuduScanAssignments) { // take care of non-optimal distribution ratios
          assignmentsForThisOperatorId.add(totalList.get(counterForLoopingTotal));
          counterForLoopingTotal += 1;
        } else {
          break;
        }
      }
    }
    return partitionAssignments;
  }
}

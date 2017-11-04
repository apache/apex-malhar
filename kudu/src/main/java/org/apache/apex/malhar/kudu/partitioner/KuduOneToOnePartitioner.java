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
 * A partitioner that assigns one to one mapping of each kudu tablet to one physical instance of Kudu input operator.
 *
 * @since 3.8.0
 */
public class KuduOneToOnePartitioner extends AbstractKuduInputPartitioner
{
  private static final Logger LOG = LoggerFactory.getLogger(KuduOneToOnePartitioner.class);

  public KuduOneToOnePartitioner(AbstractKuduInputOperator prototypeOperator)
  {
    super(prototypeOperator);
  }

  /***
   * Takes the total list of possible partition scans from a SELECT * expression and then distributes one
   * tablet per operator id. Note that the operator id is just an integer representation in this method. See
   * {@link AbstractKuduInputPartitioner.PartitioningContext#definePartitions(Collection, PartitioningContext)} where
   * this method is used to assign the plan to the actual operator instances. Please see
   * {@link AbstractKuduPartitionScanner#preparePlanForScanners(SQLToKuduPredicatesTranslator)} for details as to how
   * a query based planning is done.
   * @param totalList The ltotal list of possible tablet scans for all queries
   * @param context The context that is provided by the framework when repartitioning is to be executed
   * @return A Map of an operator identifier to the list of partition assignments.Note the Operator identifier is a
   * simple ordinal numbering of the operator and not the actual operator id.
   */
  @Override
  public Map<Integer, List<KuduPartitionScanAssignmentMeta>> assign(List<KuduPartitionScanAssignmentMeta> totalList,
      PartitioningContext context)
  {
    Map<Integer,List<KuduPartitionScanAssignmentMeta>> partitionAssignments = new HashMap<>();
    for ( int i = 0; i < totalList.size(); i++) {
      List<KuduPartitionScanAssignmentMeta> assignmentForThisPartition = new ArrayList<>();
      assignmentForThisPartition.add(totalList.get(i));
      partitionAssignments.put(i,assignmentForThisPartition);
    }
    return partitionAssignments;
  }
}

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
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.ApexKuduConnection;
import org.apache.apex.malhar.kudu.scanner.AbstractKuduPartitionScanner;
import org.apache.apex.malhar.kudu.scanner.KuduPartitionScanAssignmentMeta;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;

/**
 * An abstract class that contains logic that is common across all partitioners available for the Kudu input operator.
 *
 * @since 3.8.0
 */
public abstract class AbstractKuduInputPartitioner implements Partitioner<AbstractKuduInputOperator>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKuduInputPartitioner.class);

  @JsonIgnore
  protected AbstractKuduInputOperator prototypeKuduInputOperator;

  public AbstractKuduInputPartitioner(AbstractKuduInputOperator prototypeOperator)
  {
    prototypeKuduInputOperator = prototypeOperator;
  }

  /**
   * Builds a set of scan tokens. The list of scan tokens are generated as if the entire table is being scanned
   * i.e. a SELECT * FROM TABLE equivalent expression. This list is used to assign the partition pie assignments
   * for all of the planned partition of operators. Each operator gets a part of the PIE as if all columns were
   * selected. Subsequently when a query is to be processed, the query is used to generate the scan tokens applicable
   * for that query. Given that partition pie represents the entire data set, the scan assignments for the current
   * query will be a subset.
   * @return The list of scan tokens as if the entire table is getting scanned.
   * @throws Exception in cases when the connection to kudu cluster cannot be closed.
   */
  public List<KuduScanToken> getKuduScanTokensForSelectAllColumns() throws Exception
  {
    // We are not using the current query for deciding the partition strategy but a SELECT * as
    // we do not want to want to optimize on just the current query. This prevents rapid throttling of operator
    // instances when the scan patterns are erratic. On the other hand, this might result on under utilized
    // operator resources in the DAG but will be consistent at a minimum.
    ApexKuduConnection apexKuduConnection = prototypeKuduInputOperator.getApexKuduConnectionInfo().build();
    KuduClient clientHandle = apexKuduConnection.getKuduClient();
    KuduTable table = apexKuduConnection.getKuduTable();
    KuduScanToken.KuduScanTokenBuilder builder = clientHandle.newScanTokenBuilder(table);
    List<String> allColumns = new ArrayList<>();
    List<ColumnSchema> columnList = apexKuduConnection.getKuduTable().getSchema().getColumns();
    for ( ColumnSchema column : columnList) {
      allColumns.add(column.getName());
    }
    builder.setProjectedColumnNames(allColumns);
    LOG.debug("Building the partition pie assignments for the input operator");
    List<KuduScanToken> allPossibleTokens = builder.build();
    apexKuduConnection.close();
    return allPossibleTokens;
  }

  /***
   * Returns the number of partitions. The configuration is used as the source of truth for
   * number of partitions. The config is then overridden if the code in the client driver code explicitly sets
   * the number of partitions.
   * @param context The context as provided by the launcher
   * @return The number of partitions that are planned for this input operator. At a minimum it is 1.
   */
  public int getNumberOfPartitions(PartitioningContext context)
  {
    int proposedPartitionCount = context.getParallelPartitionCount();
    if ( prototypeKuduInputOperator.getNumberOfPartitions() != -1 ) { // -1 is the default
      // There is a manual override of partitions from code. Use this
      proposedPartitionCount = prototypeKuduInputOperator.getNumberOfPartitions();
      LOG.info(" Set the partition count based on the code as opposed to configuration ");
    }
    if ( proposedPartitionCount <= 0)  {
      // Parallel partitions not enabled. But the design is to use one to many mapping. Hence defaulting to one
      LOG.info(" Defaulting to one partition as parallel partitioning is not enabled");
      proposedPartitionCount = 1;
    }
    LOG.info(" Planning to use " + proposedPartitionCount + " partitions");
    return proposedPartitionCount;
  }

  /***
   * Builds a list of scan assignment metadata instances from raw kudu scan tokens as returned by the Kudu Query planner
   *  assuming all of the columns and rows are to be scanned
   * @param partitions The current set of partitions
   * @param context The current partitioning context
   * @return The new set of partitions
   * @throws Exception if the Kudu connection opened for generating the scan plan cannot be closed
   */
  public List<KuduPartitionScanAssignmentMeta> getListOfPartitionAssignments(
      Collection<Partition<AbstractKuduInputOperator>> partitions, PartitioningContext context) throws Exception
  {
    List<KuduPartitionScanAssignmentMeta> returnList = new ArrayList<>();
    List<KuduScanToken> allColumnsScanTokens = new ArrayList<>();
    // we are looking at a first time invocation scenario
    try {
      allColumnsScanTokens.addAll(getKuduScanTokensForSelectAllColumns());
    } catch (Exception e) {
      LOG.error(" Error while calculating the number of scan tokens for all column projections " + e.getMessage(),e);
    }
    if ( allColumnsScanTokens.size() == 0 ) {
      LOG.error("No column information could be extracted from the Kudu table");
      throw new Exception("No column information could be extracted from the Kudu table");
    }
    int totalPartitionCount = allColumnsScanTokens.size();
    LOG.info("Determined maximum as " + totalPartitionCount + " tablets for this table");
    for (int i = 0; i < totalPartitionCount; i++) {
      KuduPartitionScanAssignmentMeta aMeta = new KuduPartitionScanAssignmentMeta();
      aMeta.setOrdinal(i);
      aMeta.setTotalSize(totalPartitionCount);
      returnList.add(aMeta);
      LOG.info("A planned scan meta of the total partitions " + aMeta);
    }
    LOG.info("Total kudu partition size is " + returnList.size());
    return returnList;
  }

  /***
   * Clones the original operator and sets the partition pie assignments for this operator. Kryo is used for cloning
   * @param scanAssignmentsForThisPartition The partition pie that is assigned to the operator according to the
   *                                        configured partitioner
   * @return The partition that is used by the runtime to launch the new physical operator instance
   */
  public Partitioner.Partition<AbstractKuduInputOperator> clonePartitionAndAssignScanMeta(
      List<KuduPartitionScanAssignmentMeta> scanAssignmentsForThisPartition)
  {
    Partitioner.Partition<AbstractKuduInputOperator> clonedKuduInputOperator =
        new DefaultPartition<>(KryoCloneUtils.cloneObject(prototypeKuduInputOperator));
    clonedKuduInputOperator.getPartitionedInstance().setPartitionPieAssignment(scanAssignmentsForThisPartition);
    return clonedKuduInputOperator;
  }

  /***
   * Implements the main logic for defining partitions. The logic to create a partition is pretty straightforward. A
   * SELECT * expression is used to generate a plan ( a list of kudu scan tokens). The list is then evenly
   * distributed to each of the physical operator instances as defined by the configuration. At runtime when a new query
   * needs to be processed, a plan specific to the query is generated and the planned list of kudu scan tokens
   * for that query are distributed among the available operator instances. Note that we define the partitions only once
   * during the first invocation. This is because t is ideal we do not optimize on the current query that is running
   * before we can dynamically partition and cause too much throttling based on the query patterns. Please see
   * {@link AbstractKuduPartitionScanner#preparePlanForScanners(SQLToKuduPredicatesTranslator)} for details as to how
   * a query based planning is done.
   * @param partitions The current set of partitions
   * @param context The partitioning context as provided by the runtime platform.
   * @return The collection of planned partitions as implemented by the partitioner that is configured by the user
   */
  @Override
  public Collection<Partition<AbstractKuduInputOperator>> definePartitions(
      Collection<Partition<AbstractKuduInputOperator>> partitions, PartitioningContext context)
  {
    if ( partitions != null) {
      LOG.info("The current partitioner plan has " + partitions.size() + " operators before redefining");
    }
    List<Partition<AbstractKuduInputOperator>> partitionsForInputOperator = new ArrayList<>();
    List<KuduPartitionScanAssignmentMeta> assignmentMetaList = new ArrayList<>();
    try {
      assignmentMetaList.addAll(getListOfPartitionAssignments(partitions,context));
    } catch (Exception e) {
      throw new RuntimeException("Aborting partition planning as Kudu meta data could not be obtained", e);
    }
    LOG.info("Maximum possible Kudu input operator partition count is " + assignmentMetaList.size());
    Map<Integer,List<KuduPartitionScanAssignmentMeta>> assignments = assign(assignmentMetaList,context);
    boolean requiresRepartitioning = false;
    if ( partitions == null) {
      requiresRepartitioning = true;
    } else {
      for ( Partition<AbstractKuduInputOperator> aPartition : partitions) {
        if ( !aPartition.getPartitionedInstance().isPartitioned()) {
          requiresRepartitioning = true;
          break;
        }
      }
    }
    if ( requiresRepartitioning ) {
      partitions.clear();
      LOG.info("Clearing all of the current partitions and setting up new ones");
      partitions.clear();
      for ( int i = 0; i < assignments.size(); i++) {
        List<KuduPartitionScanAssignmentMeta> assignmentForThisOperator = assignments.get(i);
        partitionsForInputOperator.add(clonePartitionAndAssignScanMeta(assignmentForThisOperator));
        LOG.info("Assigned apex operator " + i + " with " + assignmentForThisOperator.size() + " kudu mappings");
      }
      LOG.info("Returning " + partitionsForInputOperator.size() + " partitions for the input operator");
      return partitionsForInputOperator;
    } else {
      LOG.info("Not making any changes to the partitions");
      return partitions;
    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractKuduInputOperator>> partitions)
  {

  }

  /***
   * Abstract method that will be implemented by the individual partition planners basing on the functionality they
   * provide. Please see {@link KuduOneToManyPartitioner} and {@link KuduOneToOnePartitioner} for specific
   * implementations
   * @param totalList The ltotal list of possible tablet scans for all queries
   * @param context The context that is provided by the framework when repartitioning is to be executed
   * @return A map that gives the operator number as key and the list of {@link KuduPartitionScanAssignmentMeta} that
   * are assigned for the operator with that number.
   */
  public abstract  Map<Integer, List<KuduPartitionScanAssignmentMeta>> assign(
      List<KuduPartitionScanAssignmentMeta> totalList, PartitioningContext context);

  public AbstractKuduInputOperator getPrototypeKuduInputOperator()
  {
    return prototypeKuduInputOperator;
  }

  public void setPrototypeKuduInputOperator(AbstractKuduInputOperator prototypeKuduInputOperator)
  {
    this.prototypeKuduInputOperator = prototypeKuduInputOperator;
  }
}

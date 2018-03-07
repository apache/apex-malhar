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
package org.apache.apex.malhar.python.base.partitioner;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.python.base.BasePythonExecutionOperator;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;

/***
 * Abstract partitioner that can be used in partitioning instances of the BasePythonExecution operator. This
 *  class does not do anything meaningful. See {@link ThreadStarvationBasedPartitioner} for details.
 */
public abstract class AbstractPythonExecutionPartitioner implements Partitioner<BasePythonExecutionOperator>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractPythonExecutionPartitioner.class);

  @JsonIgnore
  protected BasePythonExecutionOperator prototypePythonOperator;

  public AbstractPythonExecutionPartitioner(BasePythonExecutionOperator prototypePythonOperator)
  {
    this.prototypePythonOperator = prototypePythonOperator;
  }

  @Override
  public Collection<Partition<BasePythonExecutionOperator>> definePartitions(
      Collection<Partition<BasePythonExecutionOperator>> partitions, PartitioningContext context)
  {
    List<Partition<BasePythonExecutionOperator>> requiredPartitions = buildTargetPartitions(partitions, context);
    return requiredPartitions;
  }

  protected abstract List<Partition<BasePythonExecutionOperator>> buildTargetPartitions(
      Collection<Partition<BasePythonExecutionOperator>> partitions, PartitioningContext context);

  @Override
  public void partitioned(Map<Integer, Partition<BasePythonExecutionOperator>> partitions)
  {

  }

  public Partitioner.Partition<BasePythonExecutionOperator> clonePartition()
  {
    Partitioner.Partition<BasePythonExecutionOperator> clonedKuduInputOperator =
        new DefaultPartition<>(KryoCloneUtils.cloneObject(prototypePythonOperator));
    return clonedKuduInputOperator;
  }
}


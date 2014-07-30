/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.distributeddistinct;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.algo.UniqueValueCount.InternalCountOutput;
import com.datatorrent.lib.db.jdbc.JDBCLookupCacheBackedOperator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * <p>
 * This operator supplements the {@link UniqueValueCount} operator by making it state-full.<br/>
 * It helps to track unique values through out the lifetime of the application.
 * </p>
 * 
 * <p>
 * The operator counts the number of values emitted per key by storing previously
 * counted values in both a transient cache and in a persistent database. <br/>
 * In case of a rollback, it will erase all values committed to the database
 * in the windows greater than the activation window, then re-enter them as needed to keep it stateful.<br/>
 * This operator, when appended to {@link UniqueValueCount} will keep track of the
 * unique values emitted since the start of the application.
 *
 */
public abstract class UniqueValueCountAppender<V> extends JDBCLookupCacheBackedOperator<InternalCountOutput<V>> implements Partitioner<UniqueValueCountAppender<V>>
{
  
  protected Set<Integer> partitionKeys;
  protected int partitionMask;
  protected transient long windowID;
  protected transient boolean batch;

  public UniqueValueCountAppender()
  {
  }

  @Override
  protected void processTuple(InternalCountOutput<V> tuple)
  {

    Object key = getKeyFromTuple(tuple);
    @SuppressWarnings("unchecked")
    Set<Object> values = (Set<Object>) storeManager.get(key);
    if (values == null) {
      values = Sets.newHashSet();
    }
    values.addAll(tuple.getInternalSet());
    storeManager.put(key, values);
  }

  @Override
  protected String fetchInsertQuery()
  {
    return "INSERT INTO " + tableName + " (col1, col2, col3) VALUES (?, ?, ?)";
  }

  @Override
  protected String fetchGetQuery()
  {
    return "select col2 from " + tableName + " where col1 = ?";
  }
  
  @Override
  public Map<Object, Object> fetchStartupData()
  {
    return null;
  }

  @Override
  public void put(@Nonnull Object key, @Nonnull Object value)
  {
    try {
      batch = false;
      preparePutStatement(putStatement, key, value);
      if (batch) {
        putStatement.executeBatch();
        putStatement.clearBatch();
      }
    } catch (SQLException e) {
      throw new RuntimeException("while executing insert", e);
    }
  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void beginWindow(long windowID)
  {
    this.windowID = windowID;
  }

  @Override
  protected Object getKeyFromTuple(InternalCountOutput<V> tuple)
  {
    return tuple.getKey();
  }

  /**
   * Assigns the partitions according to certain key values and keeps track of the
   * keys that each partition will be processing so that in the case of a 
   * rollback, each partition will only clear the data that it is responsible for.
   */
  @Override
  public Collection<com.datatorrent.api.Partitioner.Partition<UniqueValueCountAppender<V>>> definePartitions(Collection<com.datatorrent.api.Partitioner.Partition<UniqueValueCountAppender<V>>> partitions, int incrementalCapacity)
  {
    if (incrementalCapacity == 0) {
      return partitions;
    }

    final int finalCapacity = partitions.size() + incrementalCapacity;
    partitions.clear();

    Collection<Partition<UniqueValueCountAppender<V>>> newPartitions = Lists.newArrayListWithCapacity(finalCapacity);

    for (int i = 0; i < finalCapacity; i++) {
      try {
        @SuppressWarnings("unchecked")
        UniqueValueCountAppender<V> statefulUniqueCount = this.getClass().newInstance();
        DefaultPartition<UniqueValueCountAppender<V>> partition = new DefaultPartition<UniqueValueCountAppender<V>>(statefulUniqueCount);
        newPartitions.add(partition);
      } catch (Throwable cause) {
        DTThrowable.rethrow(cause);
      }
    }

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), input);
    int lPartitionMask = newPartitions.iterator().next().getPartitionKeys().get(input).mask;

    for (Partition<UniqueValueCountAppender<V>> statefulUniqueCountPartition : newPartitions) {
      UniqueValueCountAppender<V> statefulUniqueCountInstance = statefulUniqueCountPartition.getPartitionedInstance();

      statefulUniqueCountInstance.partitionKeys = statefulUniqueCountPartition.getPartitionKeys().get(input).partitions;
      statefulUniqueCountInstance.partitionMask = lPartitionMask;
    }
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<UniqueValueCountAppender<V>>> partitions)
  {
  }
}

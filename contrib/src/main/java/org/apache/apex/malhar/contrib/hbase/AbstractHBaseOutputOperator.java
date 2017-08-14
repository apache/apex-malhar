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
package org.apache.apex.malhar.contrib.hbase;

import org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator;
import org.apache.hadoop.hbase.client.HTable;

import com.datatorrent.api.Operator;

/**
 * A base implementation of a HBase output operator that stores tuples in HBase and offers
 * non-transactional Insert.&nbsp; Subclasses should provide implementation for specific Hbase operations.
 *
 * @since 3.8.0
 */
public abstract class AbstractHBaseOutputOperator<T> extends AbstractStoreOutputOperator<T, HBaseStore> implements OutputAdapter.Driver<T>, Operator.CheckpointNotificationListener
{
  private transient OutputAdapter<T> outputAdapter;

  public AbstractHBaseOutputOperator()
  {
    outputAdapter = new OutputAdapter<T>(store, this);
  }

  @Override
  public void processTuple(T tuple)
  {
    outputAdapter.processTuple(tuple);
  }

  @Override
  public abstract void processTuple(T tuple, HTable table);

  /**
   * Get the table name for tuple.
   *
   * Implementations can override this method to return the table name where the tuple should be written to.
   * Return null to write to default table
   * @param tuple The tuple
   * @return The table name
   */
  @Override
  public String getTableName(T tuple)
  {
    return null;
  }

  @Override
  public void errorTuple(T tuple)
  {

  }

  @Override
  public void beforeCheckpoint(long l)
  {
    outputAdapter.flushTuples();
  }

  @Override
  public void checkpointed(long l)
  {

  }

  @Override
  public void committed(long l)
  {

  }

}

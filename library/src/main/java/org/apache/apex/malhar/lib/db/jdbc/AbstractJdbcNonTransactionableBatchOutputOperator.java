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
package org.apache.apex.malhar.lib.db.jdbc;

import java.sql.SQLException;
import java.util.List;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

/**
 * A generic output operator which updates the database without using transactions
 * and batches writes to increase performance. This operator satisfies the exactly once constraint
 * when performing updates, but may not satisfy it when doing inserts.
 * @param <T> The type of tuples to be processed.
 * @param <S> The type of store to be used.
 *
 * @since 1.0.5
 */
public abstract class AbstractJdbcNonTransactionableBatchOutputOperator<T, S extends JdbcNonTransactionalStore> extends AbstractJdbcNonTransactionableOutputOperator<T, S>
{
  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractJdbcNonTransactionableBatchOutputOperator.class);
  public static final int DEFAULT_BATCH_SIZE = 1000;

  @Min(1)
  private int batchSize = DEFAULT_BATCH_SIZE;
  private final List<T> tuples;
  private ProcessingMode mode;
  private long currentWindowId;
  private transient long committedWindowId;
  private transient String appId;
  private transient int operatorId;

  public AbstractJdbcNonTransactionableBatchOutputOperator()
  {
    tuples = Lists.newArrayList();
    batchSize = DEFAULT_BATCH_SIZE;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setMode(ProcessingMode mode)
  {
    this.mode = mode;
  }

  public ProcessingMode getMode()
  {
    return mode;
  }

  public String getAppId()
  {
    return appId;
  }

  public int getOperatorId()
  {
    return operatorId;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    mode = context.getValue(OperatorContext.PROCESSING_MODE);

    if (mode == ProcessingMode.AT_MOST_ONCE) {
      //Batch must be cleared to avoid writing same data twice
      tuples.clear();
    }

    try {
      for (T tempTuple: tuples) {
        setStatementParameters(updateCommand, tempTuple);
        updateCommand.addBatch();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    //Get the last completed window.
    committedWindowId = store.getCommittedWindowId(appId, operatorId);
    LOG.debug("AppId {} OperatorId {}", appId, operatorId);
    LOG.debug("Committed window id {}", committedWindowId);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.currentWindowId = windowId;
    LOG.debug("Committed window {}, current window {}", committedWindowId, currentWindowId);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();

    //This window is done so write it to the database.
    if (committedWindowId < currentWindowId) {
      store.storeCommittedWindowId(appId, operatorId, currentWindowId);
      committedWindowId = currentWindowId;
    }
  }

  @Override
  public void processTuple(T tuple)
  {
    //Minimize duplicated data in the atleast once case
    if (committedWindowId >= currentWindowId) {
      return;
    }

    tuples.add(tuple);

    try {
      setStatementParameters(updateCommand, tuple);
      updateCommand.addBatch();

      if (tuples.size() >= batchSize) {
        tuples.clear();
        updateCommand.executeBatch();
        updateCommand.clearBatch();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

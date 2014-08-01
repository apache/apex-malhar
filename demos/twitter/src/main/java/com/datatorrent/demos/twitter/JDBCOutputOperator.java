/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.twitter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.contrib.jdbc.JDBCOperatorBase;
import com.datatorrent.lib.db.Connectable;

/**
 * <br>Generic JDBC Output Adaptor which creates a transaction at the start of window.
 * Executes batches of sql updates and closes the transaction at the end of the window.</br>
 * <br></br>
 * <br>Each tuple corresponds to an SQL update statement. The operator groups the updates in a batch
 * and submits them with one call to the database. Batch processing improves performance considerably.</br>
 * The size of a batch is configured by batchSize property.
 * <br></br>
 * <br>The tuples in a window are stored in checkpointed collection which is cleared in the endWindow().
 * This is needed for the recovery. The operator writes a tuple exactly once in the database, which is why
 * only when all the updates are executed, the transaction is commited in the end window call.</br>
 *
 * @param <T> type of tuples </T>
 * @since 0.9.4
 */
@ShipContainingJars(classes = {JDBCOperatorBase.class, Connectable.class})
public abstract class JDBCOutputOperator<T> implements Operator
{
  public static int DEFAULT_BATCH_SIZE = 1000;
  //Checkpointed state
  @Nonnull
  protected JDBCOperatorBase jdbcConnector;
  @Min(1)
  private int batchSize;
  private final transient List<T> tuples;

  private transient long lastPersistedWindow;
  private transient long currentWindow;
  private transient int batchStartIdx;

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      if (currentWindow > lastPersistedWindow) {
        tuples.add(t);
        if ((tuples.size() - batchStartIdx) == batchSize) {
          processBatch();
        }
      }
    }
  };

  public JDBCOutputOperator()
  {
    tuples = Lists.newArrayList();
    batchSize = DEFAULT_BATCH_SIZE;
    batchStartIdx = 0;
    jdbcConnector = new JDBCOperatorBase();
  }

  private void processBatch()
  {
    try {
      PreparedStatement statement = getBatchUpdateCommandFor(tuples.subList(batchStartIdx, tuples.size()), currentWindow);
      statement.executeBatch();
      statement.clearBatch();
    }
    catch (SQLException e) {
      throw new RuntimeException("processing batch", e);
    }
    finally {
      batchStartIdx += batchSize;
    }
  }

  @Override
  public void beginWindow(long l)
  {
    currentWindow = l;
    batchStartIdx = 0;
  }

  @Override
  public void endWindow()
  {
    try {
      if (tuples.size() - batchStartIdx > 0) {
        processBatch();
      }
      updateLastCommittedWindow(currentWindow);
      jdbcConnector.getConnection().commit();
      tuples.clear();
      lastPersistedWindow = currentWindow;
    }
    catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    jdbcConnector.connect();
    try {
      jdbcConnector.getConnection().setAutoCommit(false);
      lastPersistedWindow = getLastPersistedWindow(context);
    }
    catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void teardown()
  {
    jdbcConnector.disconnect();
  }

  public void setJdbcStore(@Nonnull JDBCOperatorBase operatorBase)
  {
    this.jdbcConnector = operatorBase;
  }

  @Nonnull
  protected abstract PreparedStatement getBatchUpdateCommandFor(List<T> tuples, long windowId) throws SQLException;

  protected abstract long getLastPersistedWindow(Context.OperatorContext context) throws Exception;

  protected abstract void updateLastCommittedWindow(long window) throws Exception;

  /**
   * Sets the size of a batch metric.
   *
   * @param batchSize size of a batch
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public void handleException(Exception ex)
  {
    if (ex instanceof RuntimeException) {
      throw (RuntimeException) ex;
    }

    throw new RuntimeException(ex);
  }
}

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
package com.datatorrent.lib.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;

import com.datatorrent.lib.db.AbstractPassThruTransactionableStoreOutputOperator;

/**
 * <p>
 * Generic JDBC Output Adaptor which creates a transaction at the start of window.<br/>
 * Executes batches of sql updates and closes the transaction at the end of the window.
 * <p>
 *
 * <p>
 * Each tuple corresponds to an SQL update statement. The operator groups the updates in a batch
 * and submits them with one call to the database. Batch processing improves performance considerably.<br/>
 * The size of a batch is configured by batchSize property.
 * </p>
 *
 * <p>
 * The tuples in a window are stored in check-pointed collection which is cleared in the endWindow().
 * This is needed for the recovery. The operator writes a tuple exactly once in the database, which is why
 * only when all the updates are executed, the transaction is committed in the end window call.
 * </p>
 *
 * @displayName Abstract JDBC Transactionable Output Operator
 * @category db
 * @tags output, transactional
 *
 * @param <T> type of tuple
 * @since 0.9.4
 */
public abstract class AbstractJdbcTransactionableOutputOperator<T> extends AbstractPassThruTransactionableStoreOutputOperator<T, JdbcTransactionalStore>
{
  protected static int DEFAULT_BATCH_SIZE = 1000;

  @Min(1)
  private int batchSize;
  private final List<T> tuples;

  private transient int batchStartIdx;
  private transient PreparedStatement updateCommand;

  public AbstractJdbcTransactionableOutputOperator()
  {
    tuples = Lists.newArrayList();
    batchSize = DEFAULT_BATCH_SIZE;
    batchStartIdx = 0;
    store = new JdbcTransactionalStore();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      updateCommand = store.connection.prepareStatement(getUpdateCommand());
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void endWindow()
  {
    if (tuples.size() - batchStartIdx > 0) {
      processBatch();
    }
    super.endWindow();
    tuples.clear();
    batchStartIdx = 0;
  }

  @Override
  public void processTuple(T tuple)
  {
    tuples.add(tuple);
    if ((tuples.size() - batchStartIdx) >= batchSize) {
      processBatch();
    }
  }

  private void processBatch()
  {
    logger.debug("start {} end {}", batchStartIdx, tuples.size());
    try {
      for (int i = batchStartIdx; i < tuples.size(); i++) {
        setStatementParameters(updateCommand, tuples.get(i));
        updateCommand.addBatch();
      }
      updateCommand.executeBatch();
      updateCommand.clearBatch();
    }
    catch (SQLException e) {
      throw new RuntimeException("processing batch", e);
    }
    finally {
      batchStartIdx += tuples.size() - batchStartIdx;
    }
  }

  /**
   * Sets the size of a batch operation.<br/>
   * <b>Default:</b> {@value #DEFAULT_BATCH_SIZE}
   *
   * @param batchSize size of a batch
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  /**
   * Gets the statement which insert/update the table in the database.
   *
   * @return the sql statement to update a tuple in the database.
   */
  @Nonnull
  protected abstract String getUpdateCommand();

  /**
   * Sets the parameter of the insert/update statement with values from the tuple.
   *
   * @param statement update statement which was returned by {@link #getUpdateCommand()}
   * @param tuple     tuple
   * @throws SQLException
   */
  protected abstract void setStatementParameters(PreparedStatement statement, T tuple) throws SQLException;

  private static final Logger logger = LoggerFactory.getLogger(AbstractJdbcTransactionableOutputOperator.class);

}

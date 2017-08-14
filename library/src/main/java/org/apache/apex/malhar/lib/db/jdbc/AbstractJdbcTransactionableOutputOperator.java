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

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.AbstractPassThruTransactionableStoreOutputOperator;

import com.google.common.collect.Lists;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This is the base class implementation of a transactionable JDBC output operator.&nbsp;
 * Subclasses should implement the method which provides the insertion command.
 * <p>
 * This operator creates a transaction at the start of window, executes batches of sql updates,
 * and closes the transaction at the end of the window. Each tuple corresponds to an SQL update statement.
 * The operator groups the updates in a batch and submits them with one call to the database. Batch processing
 * improves performance considerably.<br/>
 * The size of a batch is configured by batchSize property.
 * </p>
 * <p>
 * The tuples in a window are stored in check-pointed collection which is cleared in the endWindow().
 * This is needed for the recovery. The operator writes a tuple exactly once in the database, which is why
 * only when all the updates are executed, the transaction is committed in the end window call.
 * </p>
 * @displayName Abstract JDBC Transactionable Output
 * @category Output
 * @tags transactional
 *
 * @param <T> type of tuple
 * @since 0.9.4
 */
public abstract class AbstractJdbcTransactionableOutputOperator<T>
    extends AbstractPassThruTransactionableStoreOutputOperator<T, JdbcTransactionalStore>
    implements Operator.ActivationListener<Context.OperatorContext>
{
  protected static int DEFAULT_BATCH_SIZE = 1000;

  @Min(1)
  private int batchSize;
  protected final List<T> tuples;

  protected transient int batchStartIdx;
  private transient PreparedStatement updateCommand;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<T> error = new DefaultOutputPort<>();

  @AutoMetric
  private int tuplesWrittenSuccessfully;
  @AutoMetric
  private int errorTuples;

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

  }

  @Override
  public void activate(OperatorContext context)
  {
    try {
      updateCommand = store.connection.prepareStatement(getUpdateCommand());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    tuplesWrittenSuccessfully = 0;
    errorTuples = 0;
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
  public void deactivate()
  {
  }

  @Override
  public void processTuple(T tuple)
  {
    tuples.add(tuple);
    if ((tuples.size() - batchStartIdx) >= batchSize) {
      processBatch();
    }
  }

  protected void processBatch()
  {
    logger.debug("start {} end {}", batchStartIdx, tuples.size());
    try {
      for (int i = batchStartIdx; i < tuples.size(); i++) {
        setStatementParameters(updateCommand, tuples.get(i));
        updateCommand.addBatch();
      }
      updateCommand.executeBatch();
      updateCommand.clearBatch();
      batchStartIdx += tuples.size() - batchStartIdx;
    } catch (BatchUpdateException bue) {
      logger.error(bue.getMessage());
      processUpdateCounts(bue.getUpdateCounts(), tuples.size() - batchStartIdx);
    } catch (SQLException e) {
      throw new RuntimeException("processing batch", e);
    }
  }

  /**
   * Identify which commands in the batch failed and redirect these on the error port.
   * See https://docs.oracle.com/javase/7/docs/api/java/sql/BatchUpdateException.html for more details
   *
   * @param updateCounts
   * @param commandsInBatch
   */
  protected void processUpdateCounts(int[] updateCounts, int commandsInBatch)
  {
    if (updateCounts.length < commandsInBatch) {
      // Driver chose not to continue processing after failure.
      error.emit(tuples.get(updateCounts.length + batchStartIdx));
      errorTuples++;
      // In this case, updateCounts is the number of successful queries
      tuplesWrittenSuccessfully += updateCounts.length;
      // Skip the error record
      batchStartIdx += updateCounts.length + 1;
      // And process the remaining if any
      if ((tuples.size() - batchStartIdx) > 0) {
        processBatch();
      }
    } else {
      // Driver processed all batch statements in spite of failures.
      // Pick out the failures and send on error port.
      tuplesWrittenSuccessfully = commandsInBatch;
      for (int i = 0; i < commandsInBatch; i++) {
        if (updateCounts[i] == Statement.EXECUTE_FAILED) {
          error.emit(tuples.get(i + batchStartIdx));
          errorTuples++;
          tuplesWrittenSuccessfully--;
        }
      }
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
  protected abstract String getUpdateCommand();

  /**
   * Sets the parameter of the insert/update statement with values from the tuple.
   *
   * @param statement update statement which was returned by {@link #getUpdateCommand()}
   * @param tuple     tuple
   * @throws SQLException
   */
  protected abstract void setStatementParameters(PreparedStatement statement, T tuple) throws SQLException;

  public int getTuplesWrittenSuccessfully()
  {
    return tuplesWrittenSuccessfully;
  }

  /**
   * Setter for metric tuplesWrittenSuccessfully
   * @param tuplesWrittenSuccessfully
   */
  public void setTuplesWrittenSuccessfully(int tuplesWrittenSuccessfully)
  {
    this.tuplesWrittenSuccessfully = tuplesWrittenSuccessfully;
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractJdbcTransactionableOutputOperator.class);

}

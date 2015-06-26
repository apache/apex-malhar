/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.api.Context;

import javax.annotation.Nonnull;


/**
 * <p>
 * Generic Cassandra Output Adaptor which creates a transaction at the start of window.&nbsp; Subclasses should provide implementation for getting the update statement and setting the statement parameters. <br/>
 * </p>
 *
 * <p>
 * Executes batch of CQL updates and closes the transaction at the end of the window.
 * Each tuple corresponds to an CQL update statement. The operator groups the updates in a batch
 * and submits them with one call to the database. Batch processing improves performance considerably and also provides atomicity.<br/>
 * The size of a batch is equal to the size of the window.
 * </p>
 *
 * <p>
 * The tuples in a window are stored in check-pointed collection which is cleared in the endWindow().
 * This is needed for the recovery. The operator writes a tuple exactly once in the database, which is why
 * only when all the updates are executed, the transaction is committed in the end window call.
 * </p>
 * @displayName Abstract Cassandra Transactionable Output With Prepared Statement
 * @category Store
 * @tags output operator, batch, transactionable
 * @param <T>type of tuple</T>
 * @since 1.0.2
 */
public abstract class AbstractCassandraTransactionableOutputOperatorPS<T> extends AbstractCassandraTransactionableOutputOperator<T>{

  private transient PreparedStatement updateCommand;

  /**
   * Gets the statement which insert/update the table in the database.
   *
   * @return the cql statement to update a tuple in the database.
   */
  @Nonnull
  protected abstract PreparedStatement getUpdateCommand();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    updateCommand = getUpdateCommand();
  }

  /**
   * Sets the parameter of the insert/update statement with values from the tuple.
   *
   * @param tuple     tuple
   * @return statement The statement to execute
   * @throws DriverException
   */
  protected abstract Statement setStatementParameters(PreparedStatement updateCommand, T tuple) throws DriverException;

  @Override
  protected Statement getUpdateStatement(T tuple){
    return setStatementParameters(updateCommand, tuple);
  }
}

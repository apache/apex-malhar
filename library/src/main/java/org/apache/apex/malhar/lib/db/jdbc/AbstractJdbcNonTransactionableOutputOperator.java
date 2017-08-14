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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator;

import com.datatorrent.api.Context;

/**
 * This is a base implementation of a JDBC non transactionable output operator.&nbsp;
 * Subclasses should implement the method which provides the insertion command.
 * <p></p>
 * @displayName Abstract JDBC Non Transactionable Output
 * @category Output
 * @tags jdbc
 *
 * @param <T> The kind of tuples that are being processed
 * @since 1.0.4
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractJdbcNonTransactionableOutputOperator<T, S extends JdbcStore> extends AbstractStoreOutputOperator<T, S>
{
  protected transient PreparedStatement updateCommand;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      updateCommand = store.getConnection().prepareStatement(getUpdateCommand());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets the statement parameters and executes the update
   *
   * @param tuple the tuple being processed
   */
  public void processTuple(T tuple)
  {
    try {
      setStatementParameters(updateCommand, tuple);
      updateCommand.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
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
   * @param statement
   *          update statement which was returned by {@link #getUpdateCommand()}
   * @param tuple
   *          tuple
   * @throws SQLException
   */
  protected abstract void setStatementParameters(PreparedStatement statement, T tuple) throws SQLException;
}

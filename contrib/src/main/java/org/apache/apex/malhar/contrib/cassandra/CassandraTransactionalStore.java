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
package org.apache.apex.malhar.contrib.cassandra;

import javax.annotation.Nonnull;

import org.apache.apex.malhar.lib.db.TransactionableStore;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * <p>Provides transaction support to the operators by implementing TransactionableStore abstract methods. </p>
 *
 * <p>
 * @displayName Cassandra Transactional Store
 * @category Output
 * @tags cassandra, transactional
 * @since 1.0.2
 */
public class CassandraTransactionalStore extends CassandraStore implements TransactionableStore
{
  public static String DEFAULT_APP_ID_COL = "dt_app_id";
  public static String DEFAULT_OPERATOR_ID_COL = "dt_operator_id";
  public static String DEFAULT_WINDOW_COL = "dt_window";
  public static String DEFAULT_META_TABLE = "dt_meta";

  @Nonnull
  protected String metaTableAppIdColumn;
  @Nonnull
  protected String metaTableOperatorIdColumn;
  @Nonnull
  protected String metaTableWindowColumn;
  @Nonnull
  private String metaTable;

  private transient boolean inTransaction;
  private transient PreparedStatement lastWindowFetchCommand;
  private transient PreparedStatement lastWindowUpdateCommand;
  private transient PreparedStatement lastWindowDeleteCommand;

  private transient Statement lastWindowFetchStatement;
  private transient Statement lastWindowUpdateStatement;
  private transient Statement lastWindowDeleteStatement;

  protected transient BatchStatement batchCommand;

  public CassandraTransactionalStore()
  {
    super();
    metaTable = DEFAULT_META_TABLE;
    metaTableAppIdColumn = DEFAULT_APP_ID_COL;
    metaTableOperatorIdColumn = DEFAULT_OPERATOR_ID_COL;
    metaTableWindowColumn = DEFAULT_WINDOW_COL;
    batchCommand = new BatchStatement();
    inTransaction = false;
  }

  /**
   * Sets the name of the meta table.<br/>
   * <b>Default:</b> {@value #DEFAULT_META_TABLE}
   *
   * @param metaTable meta table name.
   */
  public void setMetaTable(@Nonnull String metaTable)
  {
    this.metaTable = metaTable;
  }

  /**
   * Sets the name of app id column.<br/>
   * <b>Default:</b> {@value #DEFAULT_APP_ID_COL}
   *
   * @param appIdColumn application id column name.
   */
  public void setMetaTableAppIdColumn(@Nonnull String appIdColumn)
  {
    this.metaTableAppIdColumn = appIdColumn;
  }

  /**
   * Sets the name of operator id column.<br/>
   * <b>Default:</b> {@value #DEFAULT_OPERATOR_ID_COL}
   *
   * @param operatorIdColumn operator id column name.
   */
  public void setMetaTableOperatorIdColumn(@Nonnull String operatorIdColumn)
  {
    this.metaTableOperatorIdColumn = operatorIdColumn;
  }

  /**
   * Sets the name of the window column.<br/>
   * <b>Default:</b> {@value #DEFAULT_WINDOW_COL}
   *
   * @param windowColumn window column name.
   */
  public void setMetaTableWindowColumn(@Nonnull String windowColumn)
  {
    this.metaTableWindowColumn = windowColumn;
  }

  public Statement getLastWindowUpdateStatement()
  {
    return lastWindowUpdateStatement;
  }

  public BatchStatement getBatchCommand()
  {
    return batchCommand;
  }

  @Override
  public void connect()
  {
    super.connect();
    try {
      String command = "SELECT " + metaTableWindowColumn + " FROM " + keyspace + "." + metaTable +
          " WHERE " + metaTableAppIdColumn +
          " = ? AND " + metaTableOperatorIdColumn + " = ?";
      logger.debug(command);
      lastWindowFetchCommand = session.prepare(command);

      command = "UPDATE " + keyspace + "." + metaTable + " SET " + metaTableWindowColumn + " = ? where " + metaTableAppIdColumn + " = ? " +
          " and " + metaTableOperatorIdColumn + " = ?";
      logger.debug(command);
      lastWindowUpdateCommand = session.prepare(command);

      command = "DELETE FROM " + keyspace + "." + metaTable + " where " + metaTableAppIdColumn + " = ? and " +
          metaTableOperatorIdColumn + " = ?";
      logger.debug(command);
      lastWindowDeleteCommand = session.prepare(command);
    } catch (DriverException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void disconnect()
  {
    if (lastWindowUpdateCommand != null) {
      try {
        lastWindowUpdateCommand.disableTracing();
      } catch (DriverException e) {
        throw new RuntimeException(e);
      }
    }
    super.disconnect();
  }

  @Override
  public void beginTransaction()
  {
    inTransaction = true;
  }

  @Override
  public void commitTransaction()
  {
    session.execute(batchCommand);
    batchCommand.clear();
    inTransaction = false;
  }

  @Override
  public void rollbackTransaction()
  {
    batchCommand.clear();
    inTransaction = false;
  }

  @Override
  public boolean isInTransaction()
  {
    return inTransaction;
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    try {
      BoundStatement boundStatement = new BoundStatement(lastWindowFetchCommand);
      lastWindowFetchStatement = boundStatement.bind(appId,operatorId);
      long lastWindow = -1;
      ResultSet resultSet = session.execute(lastWindowFetchStatement);
      if (!resultSet.isExhausted()) {
        lastWindow = resultSet.one().getLong(0);
      }
      lastWindowFetchCommand.disableTracing();
      return lastWindow;
    } catch (DriverException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    try {
      BoundStatement boundStatement = new BoundStatement(lastWindowUpdateCommand);
      lastWindowUpdateStatement = boundStatement.bind(windowId,appId,operatorId);
      batchCommand.add(lastWindowUpdateStatement);
    } catch (DriverException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    try {
      BoundStatement boundStatement = new BoundStatement(lastWindowDeleteCommand);
      lastWindowDeleteStatement = boundStatement.bind(appId,operatorId);
      session.execute(lastWindowDeleteStatement);
      lastWindowDeleteCommand.disableTracing();
    } catch (DriverException e) {
      throw new RuntimeException(e);
    }
  }
}

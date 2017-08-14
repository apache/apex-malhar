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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.db.cache.AbstractDBLookupCacheBackedOperator;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;

/**
 * This is the base implementation of an operator that maintains a loading cache.&nbsp;
 * The cache is kept in a database which is connected to via JDBC.&nbsp;
 * Subclasses should implement the methods which are required to insert and retrieve data from the database.
 * <p></p>
 * @displayName JDBC Lookup Cache Backed
 * @category Input
 * @tags cache, key value
 *
 * @param <T> type of input tuples </T>
 * @since 0.9.1
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class JDBCLookupCacheBackedOperator<T> extends AbstractDBLookupCacheBackedOperator<T, JdbcStore>
{
  @NotNull
  protected String tableName;

  protected transient PreparedStatement putStatement;
  protected transient PreparedStatement getStatement;

  public JDBCLookupCacheBackedOperator()
  {
    super();
    store = new JdbcStore();
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public String getTableName()
  {
    return tableName;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    String insertQuery = fetchInsertQuery();
    String getQuery = fetchGetQuery();
    try {
      putStatement = store.connection.prepareStatement(insertQuery);
      getStatement = store.connection.prepareStatement(getQuery);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(@Nonnull Object key, @Nonnull Object value)
  {
    try {
      preparePutStatement(putStatement, key, value);
      putStatement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("while executing insert", e);
    }
  }

  @Override
  public Object get(Object key)
  {
    try {
      prepareGetStatement(getStatement, key);
      ResultSet resultSet = getStatement.executeQuery();
      return processResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException("while fetching key", e);
    }
  }

  @Override
  public List<Object> getAll(List<Object> keys)
  {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      try {
        prepareGetStatement(getStatement, key);
        ResultSet resultSet = getStatement.executeQuery();
        values.add(processResultSet(resultSet));
      } catch (SQLException e) {
        throw new RuntimeException("while fetching keys", e);
      }
    }
    return values;
  }

  protected abstract void prepareGetStatement(PreparedStatement getStatement, Object key) throws SQLException;

  protected abstract void preparePutStatement(PreparedStatement putStatement, Object key, Object value)
  throws SQLException;

  protected abstract String fetchInsertQuery();

  protected abstract String fetchGetQuery();

  protected abstract Object processResultSet(ResultSet resultSet) throws SQLException;

}

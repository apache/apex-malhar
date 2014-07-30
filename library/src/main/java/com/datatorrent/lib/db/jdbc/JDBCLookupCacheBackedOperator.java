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
package com.datatorrent.lib.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context;

import com.datatorrent.lib.db.cache.AbstractDBLookupCacheBackedOperator;

/**
 * This is {@link AbstractDBLookupCacheBackedOperator} which uses JDBC to fetch
 * the value of a key from the database when the key is not present in cache.
 * </br>
 *
 * @param <T> type of input tuples </T>
 * @since 0.9.1
 */
public abstract class JDBCLookupCacheBackedOperator<T> extends AbstractDBLookupCacheBackedOperator<T>
{
  protected final JdbcStore store;
  @NotNull
  protected String tableName;

  protected transient PreparedStatement putStatement;
  protected transient PreparedStatement getStatement;

  public JDBCLookupCacheBackedOperator()
  {	
    super();
    store = new JdbcStore();
  }

  public void setTable(String tableName)
  {
    this.tableName = tableName;
  }

  public String getTable()
  {
    return tableName;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.connect();
    String insertQuery = fetchInsertQuery();
    String getQuery = fetchGetQuery();
    try {
      putStatement = store.connection.prepareStatement(insertQuery);
      getStatement = store.connection.prepareStatement(getQuery);
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }

    super.setup(context);
  }

  @Override
  public void teardown()
  {
    store.disconnect();
    super.teardown();
  }

  public JdbcStore getStore()
  {
    return store;
  }

  @Override
  public void put(@Nonnull Object key, @Nonnull Object value)
  {
    try {
      preparePutStatement(putStatement, key, value);
      putStatement.executeUpdate();
    }
    catch (SQLException e) {
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
    }
    catch (SQLException e) {	
      throw new RuntimeException("while fetching key", e);
    }
  }

  @Override
  public Map<Object, Object> bulkGet(Set<Object> keys)
  {
    Map<Object, Object> valMap = new HashMap<Object, Object>();
    for (Object key : keys) {
      try {
        prepareGetStatement(getStatement, key);
        ResultSet resultSet = getStatement.executeQuery();
        valMap.put(key, processResultSet(resultSet));
      }
      catch (SQLException e) {
        throw new RuntimeException("while fetching keys", e);
      }
    }
    return valMap;
  }

  protected abstract void prepareGetStatement(PreparedStatement getStatement, Object key) throws SQLException;

  protected abstract void preparePutStatement(PreparedStatement putStatement, Object key, Object value) throws SQLException;

  protected abstract String fetchInsertQuery();

  protected abstract String fetchGetQuery();

  protected abstract Object processResultSet(ResultSet resultSet) throws SQLException;

}

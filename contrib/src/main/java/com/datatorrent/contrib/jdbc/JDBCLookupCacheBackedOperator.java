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
package com.datatorrent.contrib.jdbc;

import javax.annotation.Nonnull;

import com.datatorrent.lib.database.AbstractDBLookupCacheBackedOperator;
import com.datatorrent.lib.database.DBConnector;

/**
 * <br>This is {@link AbstractDBLookupCacheBackedOperator} which uses JDBC to fetch the value of a key from the database
 * when the key is not present in cache. </br>
 *
 * @param <T> type of input tuples </T>
 * @since 0.9.1
 */
public abstract class JDBCLookupCacheBackedOperator<T> extends AbstractDBLookupCacheBackedOperator<T>
{
  protected final JDBCOperatorBase jdbcConnector;

  public JDBCLookupCacheBackedOperator()
  {
    super();
    jdbcConnector = new JDBCOperatorBase();
  }

  @Nonnull
  @Override
  public DBConnector getDbConnector()
  {
    return jdbcConnector;
  }

  /**
   * Sets the database url.
   *
   * @param dbUrl url of the database.
   */
  public void setDbUrl(String dbUrl)
  {
    jdbcConnector.setDbUrl(dbUrl);
  }

  /**
   * Sets the database driver.
   *
   * @param dbDriver database driver.
   */
  public void setDbDriver(String dbDriver)
  {
    jdbcConnector.setDbDriver(dbDriver);
  }
}

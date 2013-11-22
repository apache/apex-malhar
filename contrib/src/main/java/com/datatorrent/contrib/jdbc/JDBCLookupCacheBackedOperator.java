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

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.database.CacheHandler;
import com.datatorrent.lib.util.KeyValPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

/**
 * <br>Base opertor that maintains a loading cache that has a maximum size and its entries expire after specified time.</br>
 * <br>When an extry is expired, it is loaded from the database by executing an SQL query.</br>
 *
 * @param <T> type of input tuples </T>
 * @param <K> type of keys that are parsed from the tuples </K>
 */
public abstract class JDBCLookupCacheBackedOperator<T, K> extends JDBCOperatorBase implements Operator, CacheHandler.CacheUser<K>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCLookupCacheBackedOperator.class);

  private transient CacheHandler<K> cacheHandler;

  @Min(0)
  private long maxCacheSize = 2000;

  @Min(0)
  private int entryExpiryDurationInMillis = 60000; //1 minute

  @Min(0)
  private int cacheCleanupIntervalInMillis = 60500; //.5 seconds after entries are expired

  private CacheHandler.ExpiryType entryExpiryStrategy = CacheHandler.ExpiryType.EXPIRE_AFTER_ACCESS;

  @InputPortFieldAnnotation(name = "input port")
  public transient DefaultInputPort<T> inputData = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      K key = getKeyFromTuple(tuple);
      Object value = null;
      try {
        value = cacheHandler.getCache().get(key);
      } catch (ExecutionException e) {
        logger.warn("Error retrieving value from cache ", e.fillInStackTrace());
      }
      if (value != null)
        outputPort.emit(new KeyValPair<K, Object>(key, value));
    }
  };

  @OutputPortFieldAnnotation(name = "output port")
  public transient DefaultOutputPort<KeyValPair<K, Object>> outputPort = new DefaultOutputPort<KeyValPair<K, Object>>();

  /**
   * @param maxCacheSize the max size of cache in memory.
   */
  public void setMaxCacheSize(long maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  /**
   * @param expiryType the cache entry expiry strategy.
   */
  public void setEntryExpiryStrategy(CacheHandler.ExpiryType expiryType)
  {
    this.entryExpiryStrategy = expiryType;
  }

  /**
   * @param durationInMillis the duration after which a cache entry is expired.
   */
  public void setEntryExpiryDurationInMillis(int durationInMillis)
  {
    this.entryExpiryDurationInMillis = durationInMillis;
  }

  /**
   * @param durationInMillis the duration after which cache is cleaned up regularly.
   */
  public void setCacheCleanupInMillis(int durationInMillis)
  {
    this.cacheCleanupIntervalInMillis = durationInMillis;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    setupJDBCConnection();
    cacheHandler = new CacheHandler<K>(this, maxCacheSize, entryExpiryStrategy, entryExpiryDurationInMillis, cacheCleanupIntervalInMillis);
  }

  /**
   * close JDBC connection.
   */
  @Override
  public void teardown()
  {
    closeJDBCConnection();
  }

  @Override
  public void beginWindow(long l)
  {
    //Do nothing
  }

  @Override
  public void endWindow()
  {
    //Do nothing
  }

  @Override
  @Nullable
  public Object fetchKeyFromDatabase(K key)
  {
    String query = getQueryToFetchTheKeyFromDb(key);
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      ResultSet resultSet = stmt.executeQuery(query);
      Object value = getValueFromResultSet(resultSet);
      stmt.close();
      resultSet.close();
      return value;
    } catch (SQLException e) {
      logger.warn("Error while fetching key:", e.fillInStackTrace());
    }
    return null;
  }

  public abstract K getKeyFromTuple(T tuple);

  public abstract String getQueryToFetchTheKeyFromDb(K key);

  @Nullable
  public abstract Object getValueFromResultSet(ResultSet resultSet);
}

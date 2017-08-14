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
package org.apache.apex.malhar.contrib.aerospike;

import javax.annotation.Nonnull;

import org.apache.apex.malhar.lib.db.TransactionableStore;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

/**
 * <p>Provides transaction support to the operators by implementing TransactionableStore abstract methods. </p>
 *
 * @displayName Aerospike Transactional Store
 * @category Output
 * @tags store, transactional
 * @since 1.0.4
 */
public class AerospikeTransactionalStore extends AerospikeStore implements TransactionableStore
{
  public static String DEFAULT_APP_ID_COL = "dt_app_id";
  public static String DEFAULT_OPERATOR_ID_COL = "dt_operator_id";
  public static String DEFAULT_WINDOW_COL = "dt_window";
  public static String DEFAULT_META_SET = "dt_meta";

  @Nonnull
  protected String metaTableAppIdColumn;
  @Nonnull
  protected String metaTableOperatorIdColumn;
  @Nonnull
  protected String metaTableWindowColumn;
  @Nonnull
  protected String metaSet;
  @Nonnull
  protected String namespace;

  private transient boolean inTransaction;
  private transient Statement lastWindowFetchCommand;

  public AerospikeTransactionalStore()
  {
    super();
    metaSet = DEFAULT_META_SET;
    metaTableAppIdColumn = DEFAULT_APP_ID_COL;
    metaTableOperatorIdColumn = DEFAULT_OPERATOR_ID_COL;
    metaTableWindowColumn = DEFAULT_WINDOW_COL;
    inTransaction = false;
  }

  /**
   * Sets the name of the meta set.<br/>
   * <b>Default:</b> {@value #DEFAULT_META_SET}
   *
   * @param metaSet meta set name.
   */
  public void setMetaSet(@Nonnull String metaSet)
  {
    this.metaSet = metaSet;
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

  /**
   * Sets the name of the namespace.<br/>
   *
   * @param namespace namespace.
   */
  public void setNamespace(@Nonnull String namespace)
  {
    this.namespace = namespace;
  }

  @Override
  public void connect()
  {
    super.connect();
    createIndexes();
    try {
      lastWindowFetchCommand = new Statement();
      lastWindowFetchCommand.setNamespace(namespace);
      lastWindowFetchCommand.setSetName(metaSet);
      lastWindowFetchCommand.setBinNames(metaTableWindowColumn);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void disconnect()
  {
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
    inTransaction = false;
  }

  @Override
  public void rollbackTransaction()
  {
    inTransaction = false;
  }

  @Override
  public boolean isInTransaction()
  {
    return inTransaction;
  }

  private void createIndexes()
  {
    IndexTask task;
    try {
      task = client.createIndex(null, namespace, metaSet,
          "operatorIdIndex", metaTableOperatorIdColumn, IndexType.NUMERIC);
      task.waitTillComplete();
      task = client.createIndex(null, namespace, metaSet,
          "appIdIndex", metaTableAppIdColumn, IndexType.STRING);
      task.waitTillComplete();
    } catch (AerospikeException ex) {
      throw new RuntimeException(ex);
    }

  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    try {
      lastWindowFetchCommand.setFilters(Filter.equal(metaTableOperatorIdColumn, operatorId));
      lastWindowFetchCommand.setFilters(Filter.equal(metaTableAppIdColumn, appId));
      long lastWindow = -1;
      RecordSet recordSet = client.query(null, lastWindowFetchCommand);
      while (recordSet.next()) {
        lastWindow = Long.parseLong(recordSet.getRecord().getValue(metaTableWindowColumn).toString());
      }
      return lastWindow;
    } catch (AerospikeException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    try {
      String keyString = appId + String.valueOf(operatorId);
      Key key = new Key(namespace,metaSet,keyString.hashCode());
      Bin bin1 = new Bin(metaTableAppIdColumn,appId);
      Bin bin2 = new Bin(metaTableOperatorIdColumn,operatorId);
      Bin bin3 = new Bin(metaTableWindowColumn,windowId);
      client.put(null, key, bin1,bin2,bin3);
    } catch (AerospikeException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    try {
      String keyString = appId + String.valueOf(operatorId);
      Key key = new Key(namespace,metaSet,keyString.hashCode());
      client.delete(null, key);
    } catch (AerospikeException e) {
      throw new RuntimeException(e);
    }
  }

}

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
package org.apache.apex.malhar.contrib.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.TransactionableStore;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Throwables;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * <p>Provides transaction support to the operators by implementing TransactionableStore abstract methods. </p>
 * <p>
 * Note that since HBase doesn't support transactions this store cannot guarantee each tuple is written only once to
 * HBase in case the operator is restarted from an earlier checkpoint. It only tries to minimize the number of
 * duplicates limiting it to the tuples that were processed in the window when the operator shutdown.
 * @displayName HBase Window Store
 * @category Output
 * @tags store, transactional
 * @since 1.0.2
 */
public class HBaseWindowStore extends HBaseStore implements TransactionableStore
{
  private static final transient Logger logger = LoggerFactory.getLogger(HBaseWindowStore.class);
  private static final String DEFAULT_ROW_NAME = "HBaseOperator_row";
  private static final String DEFAULT_COLUMN_FAMILY_NAME = "HBaseOutputOperator_cf";
  private static final String DEFAULT_LAST_WINDOW_PREFIX_COLUMN_NAME = "last_window";
  private transient String rowName;
  private transient String columnFamilyName;

  private transient byte[] rowBytes;
  private transient byte[] columnFamilyBytes;

  private transient String lastWindowColumnName;
  private transient byte[] lastWindowColumnBytes;

  public HBaseWindowStore()
  {
    rowName = DEFAULT_ROW_NAME;
    columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;
    lastWindowColumnName = DEFAULT_LAST_WINDOW_PREFIX_COLUMN_NAME;
    constructKeys();
  }

  private void constructKeys()
  {
    rowBytes = Bytes.toBytes(rowName);
    columnFamilyBytes = Bytes.toBytes(columnFamilyName);
  }

  /**
   * Get the row name in the table.
   *
   * @return The row name
   */
  public String getRowName()
  {
    return rowName;
  }

  /**
   * Set the row name in the table.
   *
   * @param rowName
   *            The row name
   */
  public void setRowName(String rowName)
  {
    this.rowName = rowName;
    constructKeys();
  }

  /**
   * Get the column family name in the table.
   *
   * @return The column family name
   */
  public String getColumnFamilyName()
  {
    return columnFamilyName;
  }

  /**
   * Set the column family name in the table.
   *
   * @param columnFamilyName
   *            The column family name
   */
  public void setColumnFamilyName(String columnFamilyName)
  {
    this.columnFamilyName = columnFamilyName;
    constructKeys();
  }

  @Override
  public void connect() throws IOException
  {
    super.connect();
    HTableDescriptor tdesc = table.getTableDescriptor();
    if (!tdesc.hasFamily(columnFamilyBytes)) {
      HBaseAdmin admin = new HBaseAdmin(table.getConfiguration());
      admin.disableTable(table.getTableName());
      try {
        HColumnDescriptor cdesc = new HColumnDescriptor(columnFamilyBytes);
        admin.addColumn(table.getTableName(), cdesc);
      } finally {
        admin.enableTable(table.getTableName());
        admin.close();
      }
    }
  }

  @Override
  public void beginTransaction()
  {
    // HBase does not support transactions so this method left empty
  }

  @Override
  public void commitTransaction()
  {
    try {
      flushTables();
    } catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void rollbackTransaction()
  {
    // HBase does not support transactions so this method left empty
  }

  @Override
  public boolean isInTransaction()
  {
    // HBase does not support transactions so this method left empty
    return false;
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    byte[] value = null;
    try {
      String columnKey = appId + "_" + operatorId + "_" + lastWindowColumnName;
      lastWindowColumnBytes = Bytes.toBytes(columnKey);
      Get get = new Get(rowBytes);
      get.addColumn(columnFamilyBytes, lastWindowColumnBytes);
      Result result = null;
      result = table.get(get);

      for (KeyValue kv : result.raw()) {
        if (kv.matchingQualifier(lastWindowColumnBytes)) {
          value = kv.getValue();
          break;
        }
      }
    } catch (IOException ex) {
      logger.error("Could not load window id ", ex);
      DTThrowable.rethrow(ex);
    }
    if (value != null) {
      long longval = Bytes.toLong(value);
      return longval;
    } else {
      return -1;
    }
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId,long windowId)
  {
    byte[] WindowIdBytes = Bytes.toBytes(windowId);
    String columnKey = appId + "_" + operatorId + "_" + lastWindowColumnName;
    lastWindowColumnBytes = Bytes.toBytes(columnKey);
    Put put = new Put(rowBytes);
    put.add(columnFamilyBytes, lastWindowColumnBytes, WindowIdBytes);
    try {
      table.put(put);
    } catch (RetriesExhaustedWithDetailsException e) {
      logger.error("Could not store window id ", e);
      DTThrowable.rethrow(e);
    } catch (InterruptedIOException e) {
      logger.error("Could not store window id ", e);
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    // Not applicable to hbase
  }

}

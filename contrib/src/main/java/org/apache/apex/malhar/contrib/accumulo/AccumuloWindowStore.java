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
package org.apache.apex.malhar.contrib.accumulo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.apex.malhar.lib.db.TransactionableStore;
import org.apache.hadoop.io.Text;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Provides transactional support by implementing TransactionableStore abstract methods.
 *
 * <p>
 * Not intended for true transactional
 * properties. It does not guarantee exactly once property.It only skips tuple
 * processed in previous windows
 *
 * @displayName Accumulo Window Store
 * @category Output
 * @tags accumulo, key value
 * @since 1.0.4
 */
public class AccumuloWindowStore extends AccumuloStore implements TransactionableStore
{
  private static final transient Logger logger = LoggerFactory.getLogger(AccumuloWindowStore.class);
  private static final String DEFAULT_ROW_NAME = "AccumuloOperator_row";
  private static final String DEFAULT_COLUMN_FAMILY_NAME = "AccumuloOutputOperator_cf";
  private static final String DEFAULT_LAST_WINDOW_PREFIX_COLUMN_NAME = "last_window";
  private transient String rowName;
  private transient String columnFamilyName;

  private transient byte[] rowBytes;
  private transient byte[] columnFamilyBytes;

  private transient String lastWindowColumnName;
  private transient byte[] lastWindowColumnBytes;

  public AccumuloWindowStore()
  {
    rowName = DEFAULT_ROW_NAME;
    columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;
    lastWindowColumnName = DEFAULT_LAST_WINDOW_PREFIX_COLUMN_NAME;
    constructKeys();
  }

  /**
   * the values are stored as byte arrays.This method converts string to byte
   * arrays. uses util class in hbase library to do so.
   */
  private void constructKeys()
  {
    rowBytes = rowName.getBytes();
    columnFamilyBytes = columnFamilyName.getBytes();
  }

  public String getRowName()
  {
    return rowName;
  }

  public void setRowName(String rowName)
  {
    this.rowName = rowName;
    constructKeys();
  }

  public String getColumnFamilyName()
  {
    return columnFamilyName;
  }

  public void setColumnFamilyName(String columnFamilyName)
  {
    this.columnFamilyName = columnFamilyName;
    constructKeys();
  }

  @Override
  public void beginTransaction()
  {
    // accumulo does not support transactions
  }

  @Override
  public void commitTransaction()
  {
    // accumulo does not support transactions

  }

  @Override
  public void rollbackTransaction()
  {
    // accumulo does not support transactions

  }

  @Override
  public boolean isInTransaction()
  {
    // accumulo does not support transactions
    return false;
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    byte[] value = null;
    Authorizations auths = new Authorizations();
    Scanner scan = null;
    String columnKey = appId + "_" + operatorId + "_" + lastWindowColumnName;
    lastWindowColumnBytes = columnKey.getBytes();
    try {
      scan = connector.createScanner(tableName, auths);
    } catch (TableNotFoundException e) {
      logger.error("error getting committed window id", e);
      DTThrowable.rethrow(e);
    }
    scan.setRange(new Range(new Text(rowBytes)));
    scan.fetchColumn(new Text(columnFamilyBytes), new Text(lastWindowColumnBytes));
    for (Entry<Key, Value> entry : scan) {
      value = entry.getValue().get();
    }
    if (value != null) {
      long longval = toLong(value);
      return longval;
    }
    return -1;
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId,long windowId)
  {
    byte[] WindowIdBytes = toBytes(windowId);
    String columnKey = appId + "_" + operatorId + "_" + lastWindowColumnName;
    lastWindowColumnBytes = columnKey.getBytes();
    Mutation mutation = new Mutation(rowBytes);
    mutation.put(columnFamilyBytes, lastWindowColumnBytes, WindowIdBytes);
    try {
      batchwriter.addMutation(mutation);
      batchwriter.flush();
    } catch (MutationsRejectedException e) {
      logger.error("error getting committed window id", e);
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    // accumulo does not support transactions
  }

  public static byte[] toBytes(long l)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.SIZE / 8);
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] result = null;
    try {
      dos.writeLong(l);
      result = baos.toByteArray();
      dos.close();
    } catch (IOException e) {
      logger.error("error converting to byte array");
      DTThrowable.rethrow(e);
    }
    return result;
  }

  public static long toLong(byte[] b)
  {
    ByteArrayInputStream baos = new ByteArrayInputStream(b);
    DataInputStream dos = new DataInputStream(baos);
    long result = 0;
    try {
      result = dos.readLong();
      dos.close();
    } catch (IOException e) {
      logger.error("error converting to long");
      DTThrowable.rethrow(e);
    }
    return result;
  }

}

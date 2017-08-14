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

import java.io.InterruptedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import com.google.common.base.Throwables;

/**
 * Delegating handling to an adapter so that the same adapter can be reused
 * from different inheritance hierarchies.
 *
 * @since 3.8.0
 */
public class OutputAdapter<T>
{
  private static final Logger logger = LoggerFactory.getLogger(OutputAdapter.class);

  private HBaseStore store;
  private Driver<T> driver;

  public OutputAdapter(HBaseStore store, Driver<T> driver)
  {
    this.store = store;
    this.driver = driver;
  }

  public void processTuple(T tuple)
  {
    String tableName = driver.getTableName(tuple);
    HTable table = store.getTable(tableName);
    if (table == null) {
      logger.debug("No table found for tuple {}", tuple);
      driver.errorTuple(tuple);
      return;
    }
    driver.processTuple(tuple, table);
  }

  public void flushTuples()
  {
    try {
      store.flushTables();
    } catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * A class to handle the processing of tuples independent of the operator mode, window or non-window.
   * The same Driver can be used between window and non-window operators.
   */
  interface Driver<T>
  {
    void processTuple(T tuple, HTable table);

    String getTableName(T tuple);

    void errorTuple(T tuple);
  }

}

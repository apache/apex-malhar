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
package com.datatorrent.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The abstract class implements a persistence strategy that uses application specified
 * HBase Get and Put operations to retrieve and save state.<br>
 *
 * <br>
 * The concrete persistence class that extends this class can specify how to retrieve state using
 * a HBase Get metric and save state using a HBase Put metric. The class should specifically
 * implement the operationStateGet method and return a Get metric and the operationStatePut
 * method and return a Put metric. The Get metric specifies where to retrieve the state from
 * the table and Put metric specifies where to store the state in the table.<br>
 *
 * <br>
 *
 * @since 0.3.2
 */
public abstract class HBaseBaseStatePersistence implements HBaseStatePersistenceStrategy
{

  private HTable table;

  @Override
  public void setTable(HTable table) {
    this.table = table;
  }

  @Override
  public HTable getTable() {
    return table;
  }

  @Override
  public byte[] getState(byte[] name) throws IOException
  {
    byte[] value = null;
    Get get = operationStateGet(name);
    Result result = table.get(get);
    for (KeyValue kv : result.raw()) {
      if (kv.matchingQualifier(name)) {
        value = kv.getValue();
        break;
      }
    }
    return value;
  }

  @Override
  public void saveState(byte[] name, byte[] value) throws IOException
  {
    Put put = operationStatePut(name, value);
    table.put(put);
  }

  /**
   * Return a HBase Get metric to specify where to retrieve the state of a
   * parameter from.
   * The parameter name is specified. The implementor should return a Get metric
   * that specifies where to get the last saved value of the parameter from the table.
   * @param name The parameter name
   * @return The Get metric
   */
  public abstract Get operationStateGet(byte[] name);

  /**
   * Return a HBase Put metric to specify where to save the state of a
   * parameter to.
   * The parameter name and value are specified. The implementor should return a Put
   * metric that specifies where to save the name and value of the parameter in the table.
   * @param name The parameter name
   * @param value The parameter value
   * @return The Put metric
   */
  public abstract Put operationStatePut(byte[] name, byte[] value);

}

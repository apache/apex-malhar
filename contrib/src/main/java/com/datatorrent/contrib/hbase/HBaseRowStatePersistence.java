/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Implements a persistence strategy that stores the state parameters.
 * <p>
 * State parameters are stored in column, in a specific column family and a specific row in the table.<br>
 *
 * <br>
 * The row name and column family name can be configured by setting them on the object. The
 * parameter name is used as the column name and the parameter value is used as the column value.<br>
 *
 * <br>
 * @displayName HBase Row State Persistence
 * @category Store
 * @tags get, put, persistence
 * @since 0.3.2
 */
@Deprecated
public class HBaseRowStatePersistence extends HBaseBaseStatePersistence
{

  private static final String DEFAULT_ROW_NAME = "HBaseOperator_row";
  private static final String DEFAULT_COLUMN_FAMILY_NAME = "HBaseOutputOperator_cf";

  private transient String rowName;
  private transient String columnFamilyName;

  private transient byte[] rowBytes;
  private transient byte[] columnFamilyBytes;

  public HBaseRowStatePersistence() {
    rowName = DEFAULT_ROW_NAME;
    columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;
    constructKeys();
  }

  private void constructKeys() {
    rowBytes = Bytes.toBytes(rowName);
    columnFamilyBytes = Bytes.toBytes(columnFamilyName);
  }

  /**
   * Get the row name in the table.
   * @return The row name
   */
  public String getRowName()
  {
    return rowName;
  }

  /**
   * Set the row name in the table.
   * @param rowName The row name
   */
  public void setRowName(String rowName)
  {
    this.rowName = rowName;
    constructKeys();
  }

  /**
   * Get the column family name in the table.
   * @return The column family name
   */
  public String getColumnFamilyName()
  {
    return columnFamilyName;
  }

  /**
   * Set the column family name in the table.
   * @param columnFamilyName The column family name
   */
  public void setColumnFamilyName(String columnFamilyName)
  {
    this.columnFamilyName = columnFamilyName;
    constructKeys();
  }

  @Override
  public void setup() throws IOException {
    HTable table = getTable();
    HTableDescriptor tdesc = table.getTableDescriptor();
    if (!tdesc.hasFamily(columnFamilyBytes)) {
      HBaseAdmin admin = new HBaseAdmin(table.getConfiguration());
      admin.disableTable(table.getTableName());
      try {
        HColumnDescriptor cdesc = new HColumnDescriptor(columnFamilyBytes);
        admin.addColumn(table.getTableName(), cdesc);
      } finally {
        admin.enableTable(table.getTableName());
      }
    }
  }

  @Override
  public Get operationStateGet(byte[] name) {
    Get get = new Get(rowBytes);
    get.addColumn(columnFamilyBytes, name);
    return get;
  }

  @Override
  public Put operationStatePut(byte[] name, byte[] value) {
    Put put = new Put(rowBytes);
    put.add(columnFamilyBytes, name, value);
    return put;
  }

}

/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class implements a persistence strategy that stores the state parameters in columns
 * in a specific column family and a specific row in the table.<br>
 *
 * <br>
 * The row name and column family name can be configured by setting them on the object. The
 * parameter name is used as the column name and the parameter value is used as the column value.<br>
 *
 * <br>
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseRowStatePersistence extends HBaseBaseStatePersistence
{

  private static final String DEFAULT_ROW_NAME = "hbase_outputop_row";
  private static final String DEFAULT_COLUMN_FAMILY_NAME = "hbase_outputop_cf";

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

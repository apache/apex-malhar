/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseBaseStatePersistence implements HBaseStatePersistenceStrategy
{

  private HTable table;

  public void setTable(HTable table) {
    this.table = table;
  }

  public HTable getTable() {
    return table;
  }

  @Override
  public byte[] getState(byte[] name) throws IOException
  {
    byte[] value = null;
    HTable table = getTable();
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
    HTable table = getTable();
    Put put = operationStatePut(name, value);
    table.put(put);
  }

  public abstract Get operationStateGet(byte[] name);

  public abstract Put operationStatePut(byte[] name, byte[] value);

}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBasePutOperator<T> extends HBaseOutputOperator<T>
{

  @Override
  public void processTuple(T t) throws IOException {
      HTable table = getTable();
      Put put = operationPut(t);
      table.put(put);
  }

  public abstract Put operationPut(T t);
}

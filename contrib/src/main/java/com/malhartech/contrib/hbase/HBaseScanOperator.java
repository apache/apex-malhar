/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseScanOperator<T> extends HBaseInputOperator<T>
{

  @Override
  public void emitTuples()
  {
    try {
      HTable table = getTable();
      Scan scan = operationScan();
      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        KeyValue[] kvs = result.raw();
        T t = getTuple(kvs);
        outputPort.emit(t);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected abstract Scan operationScan();

}

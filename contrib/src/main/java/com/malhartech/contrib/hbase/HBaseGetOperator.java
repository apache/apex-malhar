/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseGetOperator<T> extends HBaseInputOperator<T>
{

  @Override
  public void emitTuples()
  {
    try {
      HTable table = getTable();
      Get get = operationGet();
      Result result = table.get(get);
      //KeyValue[] kvs = result.raw();
      //T t = getTuple(kvs);
      T t = getTuple(result);
      outputPort.emit(t);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected abstract Get operationGet();

}

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
  * Operator for retrieving tuples from HBase rows.<p><br>
 *
 * <br>
 * This class provides a HBase input operator that can be used to retrieve tuples from rows in a
 * HBase table. The class should be extended by the end-operator developer. The extending class should
 * implement operationScan and getTuple methods. The operationScan method should provide a HBase Scan
 * operation object that specifies where to retrieve the tuple information from the table. The getTuple method
 * should map the contents of a Result from the Scan result to a tuple.<br>
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
        //KeyValue[] kvs = result.raw();
        //T t = getTuple(kvs);
        T t = getTuple(result);
        outputPort.emit(t);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Return a HBase Scan operation to retrieve the tuple.
   * The implementor should return a HBase Scan operation that specifies where to retrieve the tuple from
   * the table.
   *
   * @return The HBase Get operation
   */
  protected abstract Scan operationScan();

   /**
   * Get a tuple from a HBase Scan result.
   * The implementor should map the contents of a Result from a Get result and return a tuple.
   *
   * @param result The result
   * @return The tuple
   */
  protected abstract T getTuple(Result result);

}

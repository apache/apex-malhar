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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * A base implementation of hbase input operator that retrieves tuples from HBase columns and provides scan operation. <br>
 * <p>
 * <br>
 * This class provides a HBase input operator that can be used to retrieve tuples from rows in a
 * HBase table. The class should be extended by the end-operator developer. The extending class should
 * implement operationScan and getTuple methods. The operationScan method should provide a HBase Scan
 * metric object that specifies where to retrieve the tuple information from the table. The getTuple method
 * should map the contents of a Result from the Scan result to a tuple.<br>
 *
 * <br>
 * @displayName HBase Scan
 * @category Store
 * @tags scan, input operator
 * @since 0.3.2
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
   * Return a HBase Scan metric to retrieve the tuple.
   * The implementor should return a HBase Scan metric that specifies where to retrieve the tuple from
   * the table.
   *
   * @return The HBase Get metric
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

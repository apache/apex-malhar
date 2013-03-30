/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

/**
  * Operator for retrieving tuples from HBase columns.<br>
 *
 * <br>
 * This class provides a HBase input operator that can be used to retrieve tuples from columns in a
 * HBase table. The class should be extended by the end-operator developer. The extending class should
 * implement operationGet and getTuple methods. The operationGet method should provide a HBase Get operation
 * object that specifies where to retrieve tuples from a table. The getTuple method should map the contents
 * of a KeyValue from the Get result to a tuple.<br>
 *
 * <br>
 * @param <T> The tuple type
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseGetOperator<T> extends HBaseInputOperator<T>
{

  @Override
  public void emitTuples()
  {
    try {
      Get get = operationGet();
      Result result = table.get(get);
      KeyValue[] kvs = result.raw();
      //T t = getTuple(kvs);
      //T t = getTuple(result);
      for (KeyValue kv : kvs) {
        T t = getTuple(kv);
        outputPort.emit(t);
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Return a HBase Get operation to retrieve the tuple.
   * The implementor should return a HBase Get operation that specifies where to retrieve the tuple from
   * the table.
   *
   * @return The HBase Get operation
   */
  protected abstract Get operationGet();

  /**
   * Get a tuple from a HBase Get result.
   * The implementor should map the contents of a key value from a Get result and return a tuple.
   *
   * @param kv The key value
   * @return The tuple
   */
  protected abstract T getTuple(KeyValue kv);

}

/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

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
 * implement operationGet and getTuple methods. The operationGet method should provide a HBase Get metric
 * object that specifies where to retrieve tuples from a table. The getTuple method should map the contents
 * of a KeyValue from the Get result to a tuple.<br>
 *
 * <br>
 *
 * @param <T> The tuple type
 * @since 0.3.2
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
   * Return a HBase Get metric to retrieve the tuple.
   * The implementor should return a HBase Get metric that specifies where to retrieve the tuple from
   * the table.
   *
   * @return The HBase Get metric
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

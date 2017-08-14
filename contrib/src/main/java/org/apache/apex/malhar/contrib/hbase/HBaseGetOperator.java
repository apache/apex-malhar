/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

/**
 * A base implementation of hbase input operator that retrieves tuples from HBase columns and provides get operation.&nbsp; Subclasses should provide implementation for get operation. <br>
 * <p>
 * <br>
 * This class provides a HBase input operator that can be used to retrieve tuples from columns in a
 * HBase table. The class should be extended by the end-operator developer. The extending class should
 * implement operationGet and getTuple methods. The operationGet method should provide a HBase Get metric
 * object that specifies where to retrieve tuples from a table. The getTuple method should map the contents
 * of a KeyValue from the Get result to a tuple.<br>
 *
 * <br>
 * @displayName HBase Get
 * @category Input
 * @tags hbase, get
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
      Result result = getStore().getTable().get(get);
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

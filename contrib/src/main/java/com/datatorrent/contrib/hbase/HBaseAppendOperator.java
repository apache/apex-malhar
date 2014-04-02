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

import java.io.IOException;
import org.apache.hadoop.hbase.client.Append;

/**
 * Operator for storing tuples in HBase columns.<br>
 *
 *<br>
 * This class provides a HBase output operator that can be used to store tuples in columns in a
 * HBase table. It should be extended by the end-operator developer. The extending class should implement
 * operationAppend method and provide a HBase Append metric object that specifies where and what to
 * store for the tuple in the table.<br>
 *
 * <br>
 *
 * @param <T> The tuple type
 * @since 0.3.2
 */
public abstract class HBaseAppendOperator<T> extends HBaseOutputOperator<T>
{

  /**
   * Process the tuple.
   * Store the tuple in HBase. The method gets a HBase Append metric from the concrete implementation
   * and uses it to store the tuple.
   *
   * @param t The tuple
   * @throws IOException
   */
  @Override
  public void processTuple(T t) throws IOException {
      Append append = operationAppend(t);
      table.append(append);
  }

  /**
   * Return the HBase Append metric to store the tuple.
   * The implementor should return a HBase Append metric that specifies where and what to store for the tuple
   * in the table.
   *
   * @param t The tuple
   * @return The HBase Append metric
   */
  public abstract Append operationAppend(T t);
}

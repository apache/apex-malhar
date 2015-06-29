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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * Operator for storing tuples in HBase rows. <br>
 *<p>
 *<br>
 * This class provides a HBase output operator that can be used to store tuples in rows in a
 * HBase table. It should be extended by the end-operator developer. The extending class should implement
 * operationPut method and provide a HBase Put metric object that specifies where and what to store for
 * the tuple in the table.<br>
 *
 * <br>
 * @displayName HBase Put
 * @category Store
 * @tags output operator, put
 * @param <T> The tuple type
 * @since 0.3.2
 */
@Deprecated
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

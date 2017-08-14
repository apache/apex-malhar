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
package org.apache.apex.malhar.contrib.accumulo;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.apex.malhar.lib.db.AbstractStoreInputOperator;

/**
 * Base input adapter, which reads data from persistence database and writes into output port(s).&nbsp; Subclasses should provide the
 * implementation of getting the tuples and scanner methods.
 *
 * <p>
 * This is an abstract class. Sub-classes need to implement {@link #getScanner()} and {@link #getTuple(Entry)}.
 * </p>
 * @displayName Abstract Accumulo Input
 * @category Input
 * @tags key value, accumulo
 * @since 1.0.4
 */
public abstract class AbstractAccumuloInputOperator<T> extends AbstractStoreInputOperator<T, AccumuloStore>
{

  public abstract T getTuple(Entry<Key, Value> entry);

  public abstract Scanner getScanner(Connector conn);

  public AbstractAccumuloInputOperator()
  {
    store = new AccumuloStore();
  }

  @Override
  public void emitTuples()
  {
    Connector conn = store.getConnector();
    Scanner scan = getScanner(conn);

    for (Entry<Key, Value> entry : scan) {
      T tuple = getTuple(entry);
      outputPort.emit(tuple);
    }

  }

}

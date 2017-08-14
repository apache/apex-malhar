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
package org.apache.apex.malhar.lib.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is the base implementation of an input operator which consumes data from a key value store.&nbsp;
 * Subclasses should implement the method which converts key value pairs into tuples.
 * <p>
 * The default behavior is to get all the values using the keys from the store for each window. Subclasses are free to override this behavior.
 * </p>
 * @displayName Abstract Key Value Store Input
 * @category Input
 * @tags key value
 *
 * @param <T> The tuple type.
 * @param <S> The store type.
 * @since 0.9.3
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractKeyValueStoreInputOperator<T, S extends KeyValueStore> extends AbstractStoreInputOperator<T, S>
{
  protected List<Object> keys = new ArrayList<Object>();

  /**
   * Adds the key to the list of keys to be fetched for each window
   *
   * @param key a key.
   */
  public void addKey(Object key)
  {
    keys.add(key);
  }

  @Override
  public void emitTuples()
  {
    List<Object> allValues = store.getAll(keys);
    Map<Object, Object> m = new HashMap<Object, Object>();
    Iterator<Object> keyIterator = keys.iterator();
    Iterator<Object> valueIterator = allValues.iterator();
    while (keyIterator.hasNext() && valueIterator.hasNext()) {
      m.put(keyIterator.next(), valueIterator.next());
    }
    outputPort.emit(convertToTuple(m));
  }

  /**
   * Implementations should provide the way of converting from a map of key value pairs from the store to a tuple.
   *
   * @param o map of keys and values.
   * @return tuple.
   */
  public abstract T convertToTuple(Map<Object, Object> o);

}

/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.db;

import java.util.*;

/**
 * This abstract class is for any implementation of an input adapter of key value store.
 * The default behavior is to get all the values using the keys from the store for each window. Subclasses are free to override this behavior.
 *
 * @param <T> The tuple type.
 * @param <S> The store type.
 * @since 0.9.3
 */
public abstract class AbstractKeyValueStoreInputOperator<T, S extends KeyValueStore> extends AbstractStoreInputOperator<T, S>
{
  protected List<Object> keys = new ArrayList<Object>();

  /**
   * Adds the key to the list of keys to be fetched for each window
   *
   * @param key
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
   * @param o
   * @return
   */
  public abstract T convertToTuple(Map<Object, Object> o);

}

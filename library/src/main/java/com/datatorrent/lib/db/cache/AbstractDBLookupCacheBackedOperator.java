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
package com.datatorrent.lib.db.cache;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

import com.datatorrent.lib.db.Connectable;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Base operator that maintains a loading cache that has a maximum size and its entries expire after specified time.<br/>
 * Concrete implementations of this class should provide:<br/>
 * <ul>
 * <li>{@link Connectable} that holds connection parameters and setup/teardown functionality.</li>
 * <li>Method to extract a key from tuple.</li>
 * <li>Query to fetch the value of the key from tuple when the value is not present in the cache.</li>
 * </ul>
 *
 * @displayName Abstract DB Lookup Cache Backed Operator
 * @category db
 * @tags input
 *
 * @param <T> type of tuples
 * @param <S> type of store
 * @since 0.9.1
 */
public abstract class AbstractDBLookupCacheBackedOperator<T, S extends Connectable> implements Operator, CacheManager.Backup
{
  @NotNull
  protected S store;
  @NotNull
  protected CacheManager cacheManager;

  protected AbstractDBLookupCacheBackedOperator()
  {
    cacheManager = new CacheManager();
  }

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  protected void processTuple(T tuple)
  {
    Object key = getKeyFromTuple(tuple);
    Object value = cacheManager.get(key);

    if (value != null) {
      output.emit(new KeyValPair<Object, Object>(key, value));
    }
  }

  public final transient DefaultOutputPort<KeyValPair<Object, Object>> output = new DefaultOutputPort<KeyValPair<Object, Object>>();

  @Override
  public void beginWindow(long l)
  {
    //Do nothing
  }

  @Override
  public void endWindow()
  {
    //Do nothing
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    cacheManager.setBackup(this);
    try {
      cacheManager.initialize();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      cacheManager.close();
    }
    catch (IOException e) {
      LOG.error("closing manager", e);
    }
  }

  @Override
  public void connect() throws IOException
  {
    store.connect();
  }

  @Override
  public boolean connected()
  {
    return store.connected();
  }

  @Override
  public void disconnect() throws IOException
  {
    store.disconnect();
  }

  public void setStore(S store)
  {
    this.store = store;
  }

  public S getStore()
  {
    return store;
  }

  public void setCacheManager(CacheManager cacheManager)
  {
    this.cacheManager = cacheManager;
  }

  public CacheManager getCacheManager()
  {
    return cacheManager;
  }

  /**
   * <br>This operator receives tuples which encapsulates the keys. Concrete classes should
   * provide the implementation to extract a key from a tuple.</br>
   *
   * @param tuple input tuple to the operator.
   * @return key corresponding to the operator.
   */
  protected abstract Object getKeyFromTuple(T tuple);

  private final static Logger LOG = LoggerFactory.getLogger(AbstractDBLookupCacheBackedOperator.class);

}

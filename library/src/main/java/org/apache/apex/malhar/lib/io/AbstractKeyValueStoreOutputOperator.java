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
package org.apache.apex.malhar.lib.io;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the base implementation of an input operator for a key value store.&nbsp;
 * Subclasses must implement the methods used to retrieve data from and put data into the store,
 * they must also implement the methods which handle transactions.
 * <p></p>
 * @displayName Abstract Keyval Store Output
 * @category Output
 * @tags key value
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractKeyValueStoreOutputOperator<K, V> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKeyValueStoreOutputOperator.class);
  protected long currentWindowId;
  protected transient long committedWindowId = 0;
  private transient int operatorId;
  private transient String appId;
  protected Map<K, Object> dataMap = new HashMap<K, Object>();
  protected int continueOnError = 0;

  public void setContinueOnError(int continueOnError)
  {
    this.continueOnError = continueOnError;
  }

  /**
   * This input port receives tuples which are maps.
   * Each map may have many key value pairs.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Map<K, V>> input = new DefaultInputPort<Map<K, V>>()
  {
    @Override
    public void process(Map<K, V> t)
    {
      if (committedWindowId < currentWindowId) {
        AbstractKeyValueStoreOutputOperator.this.process(t);
      }
    }

  };

  /**
   * This input port receives tuples which are individual key value pairs.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<KeyValPair<K, V>> inputInd = new DefaultInputPort<KeyValPair<K, V>>()
  {
    @Override
    public void process(KeyValPair<K, V> t)
    {
      if (committedWindowId < currentWindowId) {
        AbstractKeyValueStoreOutputOperator.this.process(t.getKey(), t.getValue());
      }
    }

  };

  public abstract String get(String key);

  public abstract void put(String key, String value);

  public abstract void store(Map<K, Object> map);

  public abstract void startTransaction();

  public abstract void commitTransaction();

  public abstract void rollbackTransaction();

  public void process(Map<K, V> t)
  {
    dataMap.putAll(t);
  }

  public void process(K key, V value)
  {
    dataMap.put(key, value);
  }

  @Override
  public void setup(OperatorContext ctxt)
  {
    operatorId = ctxt.getId();
    appId = ctxt.getValue(DAGContext.APPLICATION_ID);
    String v = get(getEndWindowKey());
    if (v != null) {
      committedWindowId = Long.valueOf(v);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    dataMap.clear();
  }

  @Override
  public void endWindow()
  {
    try {
      if (committedWindowId < currentWindowId) {
        startTransaction();
        store(dataMap);
        put(getEndWindowKey(), String.valueOf(currentWindowId));
        commitTransaction();
        committedWindowId = currentWindowId;
      } else {
        LOG.info("Discarding data for window id {} because committed window is {}", currentWindowId, committedWindowId);
      }
    } catch (RuntimeException se) {
      logException("Error saving data", se);
      try {
        rollbackTransaction();
      } catch (RuntimeException re) {
        logException("Error rolling back", re);
      }
      if (continueOnError == 0) {
        throw se;
      }
    }
  }

  private String getEndWindowKey()
  {
    return "_ew:" + appId + ":" + operatorId;
  }

  private void logException(String message, Exception exception)
  {
    if (continueOnError != 0) {
      LOG.warn(message, exception);
    } else {
      LOG.error(message, exception);
    }
  }

}

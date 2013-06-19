/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.Pair;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class AbstractKeyValueStoreOutputOperator<K, V> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKeyValueStoreOutputOperator.class);
  protected long currentWindowId;
  protected transient long committedWindowId = 0;
  private transient int operatorId;
  private transient String appId;
  protected Map<K, Object> dataMap = new HashMap<K, Object>();
  @InputPortFieldAnnotation(name = "in", optional=true)
  public final transient DefaultInputPort<Map<K, V>> input = new DefaultInputPort<Map<K, V>>(this)
  {
    @Override
    public void process(Map<K, V> t)
    {
      if (committedWindowId < currentWindowId) {
        AbstractKeyValueStoreOutputOperator.this.process(t);
      }
    }

  };

  @InputPortFieldAnnotation(name = "ind", optional=true)
  public final transient DefaultInputPort<Pair<K, V>> inputInd = new DefaultInputPort<Pair<K, V>>(this)
  {
    @Override
    public void process(Pair<K, V> t)
    {
      if (committedWindowId < currentWindowId) {
        AbstractKeyValueStoreOutputOperator.this.process(t.getFirst(), t.getSecond());
      }
    }

  };

  public abstract String get(String key);

  public abstract void put(String key, String value);

  public abstract void store(Map<K, Object> map);

  public abstract void startTransaction();

  public abstract void commitTransaction();

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
    appId = ctxt.attrValue(DAGContext.APPLICATION_ID, "Unnamed");
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
    if (committedWindowId < currentWindowId) {
      startTransaction();
      store(dataMap);
      put(getEndWindowKey(), String.valueOf(currentWindowId));
      commitTransaction();
      committedWindowId = currentWindowId;
    }
    else {
      LOG.info("Discarding data for window id {} because committed window is {}", currentWindowId, committedWindowId);
    }
  }

  private String getEndWindowKey()
  {
    return "_ew:" + appId + ":" + operatorId;
  }

}

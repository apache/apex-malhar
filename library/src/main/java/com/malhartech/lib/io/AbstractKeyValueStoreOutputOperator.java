/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAGContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.util.AttributeMap;
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
  private transient long currentWindowId;
  private transient long committedWindowId = 0;
  private transient int operatorId;
  private transient String appId;
  protected transient Map<K, Object> dataMap = new HashMap<K, Object>();
  @InputPortFieldAnnotation(name = "in")
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

  public abstract String get(String key);

  public abstract void put(String key, String value);

  public abstract void store(Map<K, Object> map);

  public void process(Map<K, V> t)
  {
    dataMap.putAll(t);
  }

  @Override
  public void setup(OperatorContext ctxt)
  {
    operatorId = ctxt.getId();
    appId = ctxt.getApplicationAttributes().attr(DAGContext.STRAM_APP_ID).get();
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (committedWindowId == 0) {
      String v = get(getEndWindowKey());
      if (v != null) {
        committedWindowId = Long.valueOf(v);
        LOG.info("Committed/Current Window Ids : {} {}", committedWindowId, windowId);
      }
    }
    currentWindowId = windowId;
    dataMap.clear();
  }

  @Override
  public void endWindow()
  {
    if (committedWindowId < currentWindowId) {
      store(dataMap);
      put(getEndWindowKey(), String.valueOf(currentWindowId));
      committedWindowId = currentWindowId;
    }
  }

  private String getEndWindowKey()
  {
    return "_ew:" + appId + ":" + operatorId;
  }

}

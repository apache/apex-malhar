/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.*;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Writes out debug info to Logger<p>
 * <br>
 * Meant to handle a high throughput stream and write stats to the Logger<br>
 * <br>
 *
 */
@ModuleAnnotation(
    ports = {
  @PortAnnotation(name = DebugProbe.IPORT_INPUT, type = PortType.INPUT)
})
public class DebugProbe extends AbstractModule implements Sink
{
  private static final Logger LOG = LoggerFactory.getLogger(DebugProbe.class);
  public static final String IPORT_INPUT = Component.INPUT;
  protected HashMap<String, Integer> objcount = new HashMap<String, Integer>();
  /**
   * When set to true, toString is called on each tuple object
   */
  public static final String KEY_TOSTRING = "tostring";
  private int count = 0;
  private boolean tostring = false;

  public int getCount()
  {
    return count;
  }

  public void setString(boolean flag)
  {
    tostring = flag;
  }

  /**
   *
   * @param config
   * @throws FailedOperationException
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    tostring = config.getBoolean(KEY_TOSTRING, false);
    super.setup(config);
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object tuple)
  {
    String key = tuple.getClass().getSimpleName();
    Integer val = objcount.get(key);
    if (val == null) {
      val = new Integer(1);
    }
    else {
      val = val + 1;
    }
    objcount.put(key, val);
    count++;
    if (tostring) {
      LOG.debug("\n" + getId() + ": " + tuple.toString());
    }
  }

  @Override
  public void beginWindow()
  {
    objcount.clear();
    count = 0;
    LOG.debug(getId() + ": Begin window");
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<String, Integer> e: objcount.entrySet()) {
      LOG.debug(String.format("%s: %d tuples of type %s", getId(), e.getValue(), e.getKey()));
    }
    LOG.debug(getId() + ": End window");
  }
}

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.NodeConfiguration;
import java.util.ArrayList;
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
@NodeAnnotation(
    ports = {
        @PortAnnotation(name = DebugProbeNode.IPORT_INPUT, type = PortType.INPUT)
    }
)
public class DebugProbeNode extends AbstractNode {
  private static final Logger LOG = LoggerFactory.getLogger(DebugProbeNode.class);
  public static final String IPORT_INPUT = "input";

  protected HashMap<String, Integer> objcount = new HashMap<String, Integer>();

  /**
   * When set to true, tuples are also logged at INFO level.
   */
  public static final String P_DEBUG = "debug";

  private boolean debug;
  private int count = 0;

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public int getCount() {
    return count;
  }

  /**
   *
   * @param config
   * @throws FailedOperationException
   */
  @Override
  public void setup(NodeConfiguration config) throws FailedOperationException
  {
    super.setup(config);
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t)
  {
    String key;
    key = "unknown object type";
    if (t instanceof String) {
      key = "String";
    }
    else if (t instanceof Integer) {
      key = "Integer";
    }
    else if (t instanceof Double) {
      key = "Double";
    }
    else if (t instanceof HashMap) {
      key = "HashMap";
    }
    else if (t instanceof ArrayList) {
      key = "ArrayList";
    }
    Integer val = objcount.get(key);
    if (val == null) {
      val = new Integer(1);
    }
    else {
      val = val + 1;
    }
    objcount.put(key, val);
    count++;
}

  @Override
  public void beginWindow()
  {
    objcount.clear();
    count = 0;
    if (debug) {
      LOG.debug("Begin window");
    }
    else {
      LOG.info("Begin window");
    }
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<String, Integer> e: objcount.entrySet()) {
      if (debug) {
        LOG.debug(String.format("%d tuples of type %s", e.getValue(), e.getKey()));
      }
      else {
        LOG.info(String.format("%d tuples of type %s", e.getValue(), e.getKey()));
      }
    }
    if (debug) {
      LOG.debug("End window");
    }
    else {
      LOG.info("End window");
    }
  }
}

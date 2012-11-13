/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Writes tuples to stdout of the container<p>
 * <br>
 * Mainly to be used for debugging. Users should be careful to not have this node listen to a high throughput stream<br>
 * <br>
 *
 */
public class ConsoleOutputOperator<T> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(ConsoleOutputOperator.class);
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T t)
    {
      String s;
      if (stringFormat == null) {
        s = t.toString();
      }
      else {
        s = String.format(stringFormat, wid, t);
      }
      if (!silent) {
        System.out.println(s);
      }
      if (debug) {
        LOG.info(s);
      }
    }
  };

  private long wid = 0;
  public boolean silent = false;
  /**
   * When set to true, tuples are also logged at INFO level.
   */
  private boolean debug;
  /**
   * A formatter for {@link String#format}
   */
  private String stringFormat;

  public boolean isDebug()
  {
    return debug;
  }

  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }

  public String getStringFormat()
  {
    return stringFormat;
  }

  public void setStringFormat(String stringFormat)
  {
    this.stringFormat = stringFormat;
  }

  @Override
  public void beginWindow(long windowId)
  {
    wid = windowId;
  }


}

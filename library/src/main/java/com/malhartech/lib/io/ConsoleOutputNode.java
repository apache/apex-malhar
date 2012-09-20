/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractModule;
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
@ModuleAnnotation(
    ports = {
        @PortAnnotation(name = ConsoleOutputNode.INPUT, type = PortType.INPUT)
    }
)
public class ConsoleOutputNode extends AbstractModule
{
  private static final Logger LOG = LoggerFactory.getLogger(ConsoleOutputNode.class);

  /**
   * When set to true, tuples are also logged at INFO level.
   */
  public static final String P_DEBUG = "debug";

  /**
   * A formatter for {@link String#format}
   */
  public static final String P_STRING_FORMAT = "stringFormat";

  private boolean debug;
  private String stringFormat;

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public String getStringFormat() {
    return stringFormat;
  }

  public void setStringFormat(String stringFormat) {
    this.stringFormat = stringFormat;
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t)
  {
    if (stringFormat != null) {
      t = String.format(stringFormat, t);
    }
    System.out.println(t);
    if (debug) {
       LOG.info(""+t);
    }
  }

}

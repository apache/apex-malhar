/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.chart;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.PortContext;

/**
 * Output ports which use this type automatically record the tuples output on them so
 * that they can later be used for charting (or for debugging) purpose.
 *
 * @param <T> type of the tuple emitted on this port
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ChartingOutputPort<T> extends DefaultOutputPort<T>
{
  public ChartingOutputPort(Operator operator)
  {
    super(operator);
  }

  @Override
  public void setup(PortContext context)
  {
    context.getAttributes().attr(PortContext.AUTO_RECORD).set(true);
  }

}

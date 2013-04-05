/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Context.PortContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.PartitionableOperator;
import com.malhartech.lib.util.KeyValPair;
import com.malhartech.util.AttributeMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <T1> The type for the data points on the x-axis
 * @param <T2> The type for the data points on the y-axis
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class ChartOperator<T1, T2> extends BaseOperator implements PartitionableOperator
{
  @InputPortFieldAnnotation(name = "in1")
  public final transient DefaultInputPort<Object> in1 = new DefaultInputPort<Object>(this)
  {
    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }

  };
  @OutputPortFieldAnnotation(name = "chart")
  public final transient DefaultOutputPort<KeyValPair<T1, T2>> chart = new DefaultOutputPort<KeyValPair<T1, T2>>(this);

  @Override
  public void setup(OperatorContext context)
  {
    AttributeMap<PortContext> outputPortAttributes = context.getOutputPortAttributes("chart");
    if (outputPortAttributes != null) {
      outputPortAttributes.attr(PortContext.AUTO_RECORD).set(true);
    }
  }

  @Override
  public void endWindow()
  {
    T1 x = getX();
    T2 y = getY();
    if (x != null && y != null) {
      chart.emit(new KeyValPair<T1, T2>(x, y));
    }
  }

  public abstract T1 getX();

  public abstract T2 getY();

  public abstract void processTuple(Object tuple);

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity)
  {
    // prevent partitioning
    List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(1);
    newPartitions.add(partitions.iterator().next());
    return newPartitions;
  }

  private static final Logger logger = LoggerFactory.getLogger(ChartOperator.class);
}

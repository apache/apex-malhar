/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Context.PortContext;
import com.malhartech.api.PartitionableOperator;
import com.malhartech.util.AttributeMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class ChartOperator extends BaseOperator implements PartitionableOperator
{
  public enum Type
  {
    LINE,
    CANDLE,
    ENUM, // TBD
    HISTOGRAM, // TBD
  }

  public abstract Type getChartType();

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity)
  {
    // prevent partitioning
    List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(1);
    newPartitions.add(partitions.iterator().next());
    return newPartitions;
  }

  @Override
  public void setup(OperatorContext context)
  {
    AttributeMap<PortContext> outputPortAttributes = context.getOutputPortAttributes("chart");
    if (outputPortAttributes != null) {
      outputPortAttributes.attr(PortContext.AUTO_RECORD).set(true);
    }
  }
}

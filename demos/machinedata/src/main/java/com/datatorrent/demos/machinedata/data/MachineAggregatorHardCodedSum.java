/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import com.datatorrent.lib.dimensions.DimensionsDescriptor;

/**
 * @since 3.2.0
 */
public class MachineAggregatorHardCodedSum extends AbstractMachineAggregatorHardcoded
{
  private static final long serialVersionUID = 201510230657L;

  private MachineAggregatorHardCodedSum()
  {
    //for kryo
  }

  public MachineAggregatorHardCodedSum(int ddID, DimensionsDescriptor dimensionsDescriptor)
  {
    super(ddID, dimensionsDescriptor);
  }

  @Override
  public MachineHardCodedAggregate getGroup(MachineInfo src, int aggregatorIndex)
  {
    MachineHardCodedAggregate aggregate = super.getGroup(src, aggregatorIndex);
    aggregate.sum = true;

    return aggregate;
  }

  @Override
  public void aggregate(MachineHardCodedAggregate dest, MachineInfo src)
  {
    dest.cpuUsage += src.getCpu();
    dest.hddUsage += src.getHdd();
    dest.ramUsage += src.getRam();
  }

  @Override
  public void aggregate(MachineHardCodedAggregate dest, MachineHardCodedAggregate src)
  {
    dest.cpuUsage += src.cpuUsage;
    dest.hddUsage += src.hddUsage;
    dest.ramUsage += src.ramUsage;
  }

}

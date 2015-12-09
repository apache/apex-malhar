/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import com.datatorrent.lib.dimensions.DimensionsDescriptor;

/**
 * @since 3.2.0
 */
public class MachineAggregatorHardCodedCount extends AbstractMachineAggregatorHardcoded
{
  private static final long serialVersionUID = 102220150244L;

  private MachineAggregatorHardCodedCount()
  {
    //for kryo
  }

  public MachineAggregatorHardCodedCount(int ddID, DimensionsDescriptor dimensionsDescriptor)
  {
    super(ddID, dimensionsDescriptor);
  }

  @Override
  public void aggregate(MachineHardCodedAggregate dest, MachineInfo src)
  {
    dest.count++;
  }

  @Override
  public void aggregate(MachineHardCodedAggregate dest, MachineHardCodedAggregate src)
  {
    dest.count += src.count;
  }

}

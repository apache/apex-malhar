/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.Name;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.dimensions.aggregator.AbstractIncrementalAggregator;
import com.datatorrent.lib.dimensions.aggregator.AggregatorCount;

/**
 * This is a custom aggregator to speed up computation for the machine demo.
 * @since 3.2.0
 */
@Name("COUNT")
public class MachineAggregatorCount extends AbstractIncrementalAggregator
{
  private static final long serialVersionUID = 201510120415L;

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    src.used = true;
    int[] stringIndexSubset = this.context.indexSubsetKeys.fieldsStringIndexSubset;

    String[] keys;

    if (stringIndexSubset == null) {
      keys = new String[0];
    } else {
      keys = new String[stringIndexSubset.length];
    }

    for (int counter = 0; counter < keys.length; counter++) {
      keys[counter] = src.getKeys().getFieldsString()[stringIndexSubset[counter]];
    }

    MachineAggregate machineAggregate = new MachineAggregate(keys,
                                                             0,
                                                             context.schemaID,
                                                             context.dimensionsDescriptorID,
                                                             context.aggregatorID,
                                                             0L,
                                                             0L,
                                                             0L,
                                                             this.context.dd.getCustomTimeBucket().roundDown(src.getKeys().getFieldsLong()[0]),
                                                             this.context.customTimeBucketRegistry.getTimeBucketId(this.context.dd.getCustomTimeBucket()));

    machineAggregate.setAggregatorIndex(aggregatorIndex);
    return machineAggregate;
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return AggregatorCount.TYPE_CONVERSION_MAP.get(inputType);
  }

  @Override
  public FieldsDescriptor getMetaDataDescriptor()
  {
    return null;
  }

  @Override
  public void aggregate(Aggregate dest, InputEvent src)
  {
    ((MachineAggregate)dest).cpuUsage++;
    ((MachineAggregate)dest).hddUsage++;
    ((MachineAggregate)dest).ramUsage++;
  }

  @Override
  public void aggregate(Aggregate dest, Aggregate src)
  {
    ((MachineAggregate)dest).cpuUsage += ((MachineAggregate)src).cpuUsage;
    ((MachineAggregate)dest).hddUsage += ((MachineAggregate)src).hddUsage;
    ((MachineAggregate)dest).ramUsage += ((MachineAggregate)src).ramUsage;
  }

  private static final Logger LOG = LoggerFactory.getLogger(MachineAggregatorCount.class);
}

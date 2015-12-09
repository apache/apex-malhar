/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import java.util.Objects;

import com.datatorrent.demos.machinedata.data.MachineKey.KeySelector;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;

/**
 * @since 3.2.0
 */
public abstract class AbstractMachineAggregatorHardcoded implements Aggregator<MachineInfo, MachineHardCodedAggregate>
{
  private static final long serialVersionUID = 201510220615L;

  protected KeySelector ks;

  protected int ddID;
  private DimensionsDescriptor dimensionsDescriptor;

  protected AbstractMachineAggregatorHardcoded()
  {
    //for kryo
  }

  public AbstractMachineAggregatorHardcoded(int ddID, DimensionsDescriptor dimensionsDescriptor)
  {
    this.ddID = ddID;
    this.dimensionsDescriptor = dimensionsDescriptor;
    initialize();
  }

  private void initialize()
  {
    ks = new KeySelector(dimensionsDescriptor.getFields().getFields());
  }

  /**
   * @return the ddID
   */
  public int getDdID()
  {
    return ddID;
  }

  /**
   * @param ddID the ddID to set
   */
  public void setDdID(int ddID)
  {
    this.ddID = ddID;
  }

  /**
   * @return the dimensionsDescriptor
   */
  public DimensionsDescriptor getDimensionsDescriptor()
  {
    return dimensionsDescriptor;
  }

  /**
   * @param dimensionsDescriptor the dimensionsDescriptor to set
   */
  public void setDimensionsDescriptor(DimensionsDescriptor dimensionsDescriptor)
  {
    this.dimensionsDescriptor = dimensionsDescriptor;
  }

  @Override
  public MachineHardCodedAggregate getGroup(MachineInfo src, int aggregatorIndex)
  {
    MachineKey key = src.getMachineKey();
    MachineHardCodedAggregate aggregate = new MachineHardCodedAggregate();

    if (ks.useCustomer) {
      aggregate.customer = key.getCustomer();
    }

    if (ks.useProduct) {
      aggregate.product = key.getProduct();
    }

    if (ks.useOs) {
      aggregate.os = key.getOs();
    }

    if (ks.useSoftware1) {
      aggregate.software1 = key.getSoftware1();
    }

    if (ks.useSoftware2) {
      aggregate.software2 = key.getSoftware2();
    }

    if (ks.useDeviceId) {
      aggregate.deviceId = key.getDeviceId();
    }

    aggregate.timestamp = dimensionsDescriptor.getCustomTimeBucket().roundDown(key.getTimestamp());
    aggregate.count = 0L;
    aggregate.cpuUsage = 0L;
    aggregate.ramUsage = 0L;
    aggregate.hddUsage = 0L;
    aggregate.ddID = ddID;
    aggregate.aggregatorIndex = aggregatorIndex;

    return aggregate;
  }

  @Override
  public abstract void aggregate(MachineHardCodedAggregate dest, MachineInfo src);

  @Override
  public abstract void aggregate(MachineHardCodedAggregate dest, MachineHardCodedAggregate src);

  @Override
  public int computeHashCode(MachineInfo t)
  {
    MachineKey key = t.getMachineKey();
    int hash = key.hashCode(ks);
    hash = hash * 67 + (int) dimensionsDescriptor.getCustomTimeBucket().roundDown(key.getTimestamp());
    return hash;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(this.ks,
                        this.ddID,
                        this.dimensionsDescriptor);
  }

  @Override
  public boolean equals(MachineInfo t, MachineInfo t1)
  {
    MachineKey keyA = t.getMachineKey();
    MachineKey keyB = t1.getMachineKey();

    if (!keyA.equalsWithKey(keyB, ks)) {
      return false;
    }

    return dimensionsDescriptor.getCustomTimeBucket().roundDown(keyA.getTimestamp())
           == dimensionsDescriptor.getCustomTimeBucket().roundDown(keyB.getTimestamp());
  }
}

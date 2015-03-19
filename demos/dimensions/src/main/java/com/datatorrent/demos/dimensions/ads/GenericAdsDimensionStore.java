/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.contrib.dimensions.GenericDimensionsStoreAIHDHT;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.datatorrent.lib.appdata.dimensions.AggregatorSum;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericAdsDimensionStore extends GenericDimensionsStoreAIHDHT<AdInfoAggregateEvent>
{
  private transient AggregatorSum aggregator = new AggregatorSum();

  public GenericAdsDimensionStore()
  {
  }

  @Override
  public DimensionsAggregator<GenericAggregateEvent> getAggregator(int aggregatorID)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getBucketForSchema(int schemaID)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getPartitionGAE(GenericAggregateEvent inputEvent)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.hdht;

import com.datatorrent.ada.counters.DataGroup;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class BucketIdentifier
{
  private DataGroup datagroup;
  private DimensionsDescriptor aggregatorDescriptor;

  public BucketIdentifier(DataGroup datagroup,
                          DimensionsDescriptor aggregationDescriptor)
  {
    setDatagroup(datagroup);
    setAggregatorDescriptor(aggregationDescriptor);
  }

  /**
   * @return the aggregationDescriptor
   */
  public DimensionsDescriptor getAggregatorDescriptor()
  {
    return aggregatorDescriptor;
  }

  /**
   * @param aggregationDescriptor the aggregationDescriptor to set
   */
  private void setAggregatorDescriptor(DimensionsDescriptor aggregatorDescriptor)
  {
    Preconditions.checkNotNull(aggregatorDescriptor);
    this.aggregatorDescriptor = aggregatorDescriptor;
  }

  /**
   * @return the datagroup
   */
  public DataGroup getDatagroup()
  {
    return datagroup;
  }

  /**
   * @param datagroup the datagroup to set
   */
  public void setDatagroup(DataGroup datagroup)
  {
    Preconditions.checkNotNull(datagroup);
    this.datagroup = datagroup;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 97 * hash + (this.datagroup != null ? this.datagroup.hashCode() : 0);
    hash = 97 * hash + (this.aggregatorDescriptor != null ? this.aggregatorDescriptor.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null) {
      return false;
    }
    if(getClass() != obj.getClass()) {
      return false;
    }
    final BucketIdentifier other = (BucketIdentifier)obj;
    
    if(this.datagroup != other.datagroup && (this.datagroup == null || !this.datagroup.equals(other.datagroup))) {
      return false;
    }
    if(this.aggregatorDescriptor != other.aggregatorDescriptor && (this.aggregatorDescriptor == null || !this.aggregatorDescriptor.equals(other.aggregatorDescriptor))) {
      return false;
    }
    return true;
  }
}

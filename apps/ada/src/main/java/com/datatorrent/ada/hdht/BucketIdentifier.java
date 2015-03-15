/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.hdht;

import com.datatorrent.ada.counters.DataGroup;
import com.datatorrent.lib.appdata.dimensions.AggregatorDescriptor;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class BucketIdentifier extends DataGroup
{
  private String appID;
  private AggregatorDescriptor aggregatorDescriptor;

  public BucketIdentifier(String user,
                          String appName,
                          String logicalOperatorName,
                          String version,
                          String appID,
                          AggregatorDescriptor aggregationDescriptor)
  {
    super(user,
          appName,
          logicalOperatorName,
          version);
    setAppID(appID);
    setAggregatorDescriptor(aggregationDescriptor);
  }

  /**
   * @return the appID
   */
  public String getAppID()
  {
    return appID;
  }

  /**
   * @param appID the appID to set
   */
  private void setAppID(String appID)
  {
    Preconditions.checkNotNull(appID);
    this.appID = appID;
  }

  /**
   * @return the aggregationDescriptor
   */
  public AggregatorDescriptor getAggregatorDescriptor()
  {
    return aggregatorDescriptor;
  }

  /**
   * @param aggregationDescriptor the aggregationDescriptor to set
   */
  private void setAggregatorDescriptor(AggregatorDescriptor aggregatorDescriptor)
  {
    Preconditions.checkNotNull(aggregatorDescriptor);
    this.aggregatorDescriptor = aggregatorDescriptor;
  }
}

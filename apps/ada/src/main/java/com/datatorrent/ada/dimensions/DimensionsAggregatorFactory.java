/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.dimensions;

import com.datatorrent.lib.appdata.dimensions.AggregatorDescriptor;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.datatorrent.lib.appdata.dimensions.GenericEvent;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface DimensionsAggregatorFactory
{
  public DimensionsAggregator<GenericEvent, GenericAggregateEvent> create(AggregatorDescriptor ad);
}

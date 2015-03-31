/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersStore extends AbstractSinglePortHDHTWriter<GenericAggregateEvent>
{
  public transient final DefaultInputPort<CountersData> query = new DefaultInputPort<CountersData>() {
    @Override
    public void process(CountersData tuple)
    {
      if(tuple instanceof CountersSchema) {
        CountersSchemaUtils.addSchema((CountersSchema) tuple);
      }
      else if(tuple instanceof CountersUpdateDataLogical) {

      }
      else {
        throw new UnsupportedOperationException("Usupported tuple type");
      }
    }
  };

  public CountersStore()
  {
  }

  @Override
  protected HDHTCodec<GenericAggregateEvent> getCodec()
  {
    return null;
  }
}

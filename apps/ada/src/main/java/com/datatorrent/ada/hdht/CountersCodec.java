/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.hdht;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter.HDHTCodec;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.datatorrent.lib.appdata.gpo.GPOUtils;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersCodec implements HDHTCodec<GenericAggregateEvent>
{
  public CountersCodec()
  {
  }

  @Override
  public synchronized byte[] getKeyBytes(GenericAggregateEvent event)
  {
    return GPOUtils.serialize(event.getKeys());
  }

  @Override
  public synchronized byte[] getValueBytes(GenericAggregateEvent event)
  {
    return GPOUtils.serialize(event.getAggregates());
  }

  @Override
  public GenericAggregateEvent fromKeyValue(Slice key, byte[] value)
  {
    return null;
  }

  @Override
  public Object fromByteArray(Slice fragment)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Slice toByteArray(GenericAggregateEvent o)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getPartition(GenericAggregateEvent o)
  {
    //TODO this may change later
    return 0;
  }
}

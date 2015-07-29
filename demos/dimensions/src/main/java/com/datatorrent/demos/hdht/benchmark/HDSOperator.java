/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.hdht.benchmark;

import com.datatorrent.netlet.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.contrib.hdht.MutableKeyValue;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

import java.io.IOException;
import java.util.Arrays;

/**
 * HDSOperator
 *
 * @displayName HDHT Operator
 * @category Output
 * @tags hdht
 * @since 2.0.0
 */
public class HDSOperator extends AbstractSinglePortHDHTWriter<MutableKeyValue>
{
  public boolean isReadModifyWriteMode()
  {
    return readModifyWriteMode;
  }

  public void setReadModifyWriteMode(boolean readModifyWriteMode)
  {
    this.readModifyWriteMode = readModifyWriteMode;
  }

  private boolean readModifyWriteMode = false;

  public static class MutableKeyValCodec extends KryoSerializableStreamCodec<MutableKeyValue> implements HDHTCodec<MutableKeyValue>
  {
    @Override public byte[] getKeyBytes(MutableKeyValue mutableKeyValue)
    {
      return mutableKeyValue.getKey();
    }

    @Override public byte[] getValueBytes(MutableKeyValue mutableKeyValue)
    {
      return mutableKeyValue.getValue();
    }

    @Override public MutableKeyValue fromKeyValue(Slice key, byte[] value)
    {
      MutableKeyValue pair = new MutableKeyValue(null, null);
      pair.setKey(key.buffer);
      pair.setValue(value);
      return pair;
    }

    @Override public int getPartition(MutableKeyValue tuple)
    {
      return Arrays.hashCode(tuple.getKey());
    }
  }


  @Override protected HDHTCodec<MutableKeyValue> getCodec()
  {
    return new MutableKeyValCodec();
  }

  @Override protected void processEvent(MutableKeyValue event) throws IOException
  {
    if (readModifyWriteMode) {
      // do get and then put to simulate read-modify-write workload.
      byte[] oldval = super.get(getBucketKey(event), new Slice(event.getKey()));
      if (oldval != null) {
        // Modify event.
        byte[] newval = event.getValue();
        for (int i = 0; i < newval.length; i++)
          if (i < newval.length)
            oldval[i] += newval[i];
        event.setValue(oldval);
      }
    }
    super.processEvent(event);
  }

}

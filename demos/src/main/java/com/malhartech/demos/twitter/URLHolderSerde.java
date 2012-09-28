/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.dag.Module;
import com.malhartech.dag.SerDe;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class URLHolderSerde implements SerDe
{
  private static final Logger logger = LoggerFactory.getLogger(URLHolderSerde.class);
  ByteBuffer bb = ByteBuffer.allocate(256);

  @Override
  public Object fromByteArray(byte[] bytes)
  {
    bb.clear();
    bb.put(bytes);
    bb.rewind();

    int totalCount = bb.getInt();

    ByteBuffer newbb = ByteBuffer.allocate(bb.getInt());
    newbb.put(bb.array(), bb.position(), newbb.remaining());
    newbb.flip();

    WindowedHolder wuh = new WindowedHolder(newbb, 0);
    wuh.totalCount = totalCount;

    return wuh;
  }

  @Override
  public byte[] toByteArray(Object o)
  {
    return null;
//    WindowedHolder wuh = (WindowedHolder) o;
//
//    bb.clear();
//    bb.putInt(wuh.totalCount);
//
//    byte[] url = wuh.identifier.array();
//    int length = url.length;
//    if (length > 248) {
//      length = 248;
//    }
//
//    bb.putInt(length);
//    bb.put(url, 0, length);
//
//    byte[] retvalue = new byte[length + 8];
//    bb.rewind();
//    bb.get(retvalue);
//
//    return retvalue;
  }

  @Override
  public byte[] getPartition(Object o)
  {
    return null;
  }

  @Override
  public byte[][] getPartitions()
  {
    return null;
  }

  @Override
  public boolean transferState(Module destination, Module source, Collection<byte[]> partitions)
  {
    return false;
  }
}

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.api.StreamCodec;
import com.malhartech.common.Fragment;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class URLSerDe implements StreamCodec<byte[]>
{
  private static final Logger logger = LoggerFactory.getLogger(URLSerDe.class);

  /**
   * Covert the bytes into object useful for downstream node.
   *
   * @return WindowedURLHolder object which represents the bytes.
   */
  @Override
  public byte[] fromByteArray(DataStatePair dspair)
  {
    Fragment f = dspair.data;
    if (f == null || f.buffer == null) {
      return null;
    }
    else if (f.offset == 0 && f.length == f.buffer.length) {
      return f.buffer;
    }
    else {
      byte[] buffer = new byte[f.buffer.length];
      System.arraycopy(f.buffer, f.offset, buffer, 0, f.length);
      return buffer;
    }
  }

  /**
   * Cast the input object to byte[].
   *
   * @param o - byte array representing the bytes of the string
   * @return the same object as input
   */
  @Override
  public DataStatePair toByteArray(byte[] o)
  {
    DataStatePair dspair = new DataStatePair();
    dspair.data = new Fragment(o, 0, o.length);
    dspair.state = null;
    return dspair;
  }

  @Override
  public int getPartition(byte[] o)
  {
    ByteBuffer bb = ByteBuffer.wrap(o);
    return bb.hashCode();
  }

  @Override
  public void resetState()
  {
    /* there is nothing to reset in this serde */
  }

}

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
  /**
   * Covert the bytes into object useful for downstream node.
   *
   * @param fragment
   * @return WindowedURLHolder object which represents the bytes.
   */
  @Override
  public byte[] fromByteArray(Fragment fragment)
  {
    if (fragment == null || fragment.buffer == null) {
      return null;
    }
    else if (fragment.offset == 0 && fragment.length == fragment.buffer.length) {
      return fragment.buffer;
    }
    else {
      byte[] buffer = new byte[fragment.buffer.length];
      System.arraycopy(fragment.buffer, fragment.offset, buffer, 0, fragment.length);
      return buffer;
    }
  }

  /**
   * Cast the input object to byte[].
   *
   * @param object - byte array representing the bytes of the string
   * @return the same object as input
   */
  @Override
  public Fragment toByteArray(byte[] object)
  {
    return new Fragment(object, 0, object.length);
  }

  @Override
  public int getPartition(byte[] object)
  {
    ByteBuffer bb = ByteBuffer.wrap(object);
    return bb.hashCode();
  }

  private static final Logger logger = LoggerFactory.getLogger(URLSerDe.class);
}

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.api.Operator;
import com.malhartech.api.StreamCodec;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class URLSerDe implements StreamCodec<byte[]>
{
  private static final Logger logger = LoggerFactory.getLogger(URLSerDe.class);
  private static final byte[][] partitions = new byte[][]{
    //    {0x0},
    //    {1},
    //    {2},
    //    {3},
    //    {4},
    //    {5},
    //    {6},
    //    {7},
    {'a'},
    {'b'}
  };
  private static final int partitionsCount = partitions.length;

  /**
   * Covert the bytes into object useful for downstream node.
   *
   * @param bytes constituents of the String representation of the URL.
   * @return WindowedURLHolder object which represents the bytes.
   */
  @Override
  public byte[] fromByteArray(DataStatePair dspair)
  {
    return dspair.data;
  }

  /**
   * Cast the input object to byte[].
   * @param o - byte array representing the bytes of the string
   * @return the same object as input
   */
  @Override
  public DataStatePair toByteArray(byte[] o)
  {
    DataStatePair dspair = new DataStatePair();
    dspair.data = o;
    dspair.state = null;
    return dspair;
  }

  @Override
  public byte[] getPartition(Object o)
  {
    ByteBuffer bb = ByteBuffer.wrap((byte[]) o);
    return partitions[Math.abs(bb.hashCode()) % partitionsCount];
  }

  public byte[][] getPartitions()
  {
    return partitions;
  }

  @Override
  public void checkpoint()
  {
    /* there is nothing to reset in this serde */
  }
}

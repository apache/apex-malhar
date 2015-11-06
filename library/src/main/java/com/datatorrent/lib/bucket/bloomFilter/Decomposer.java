/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket.bloomFilter;

import java.nio.charset.Charset;

/**
 * Contract for implementations that wish to help in decomposing an object to a
 * byte-array so that various hashes can be computed over the same.
 *
 * @param <T>
 *          the type of object over which this decomposer works
 */
public interface Decomposer<T>
{

  /**
   * Decompose the object into the byte-array
   *
   * @param object
   *          the object to be decomposed
   */
  byte[] decompose(T object);

  public class DefaultDecomposer<T> implements Decomposer<T>
  {
    /**
     * The default platform encoding
     */
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    /**
     * Decompose the object
     */
    @Override
    public byte[] decompose(T object)
    {
      if (object == null) {
        return null;
      }
      if (object instanceof String) {
        return (((String)object).getBytes(DEFAULT_CHARSET));
      }
      return (object.toString().getBytes(DEFAULT_CHARSET));
    }

  }
}

/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.engine.DefaultStreamCodec;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class Codec<K, V> extends DefaultStreamCodec<KeyValPair<K, V>>
{
  /**
   * A codec to enable partitioning to be done by key
   */
  @Override
  public int getPartition(KeyValPair<K, V> o)
  {
    return o.getKey().hashCode();
  }
}

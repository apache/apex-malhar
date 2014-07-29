/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.IOException;
import java.util.Map;

public interface HDS
{
  interface DataKey {
    /**
     * The bucket key.
     * @return
     */
    long getBucketKey();
    /**
     * Monotonically increasing sequence that is used as secondary level of organization within buckets.
     * In most cases this will be a time stamp.
     * @return
     */
    long getSequence();
  }

  interface Bucket<K extends DataKey, V>
  {
    void put(Map.Entry<K, V> entry) throws IOException;
    V get(K key) throws IOException;
  }
}

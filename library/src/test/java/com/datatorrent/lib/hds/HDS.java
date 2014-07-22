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
    String getBucketKey();
  }

  interface TimeSeriesDataKey extends DataKey {
    long getTime();
  }

  interface Bucket<K extends DataKey, V>
  {
    void put(Map.Entry<K, V> entry) throws IOException;
    V get(K key) throws IOException;
  }
}

/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.IOException;

public interface HDS
{
  interface BucketManager
  {
    void put(long bucketKey, byte[] key, byte[] value) throws IOException;
    byte[] get(long bucketKey, byte[] key) throws IOException;
  }
}

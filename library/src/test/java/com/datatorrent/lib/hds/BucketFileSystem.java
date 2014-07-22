/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import com.datatorrent.lib.hds.HDS.DataKey;

/**
 * Encapsulate management of meta information and underlying file system interaction.
 */
public interface BucketFileSystem
{

  public class BucketFileMeta
  {
    String name;
    int size;
    long minTime;
    long maxTime;
  }

  BucketFileMeta createFile(DataKey key, long minTime, long maxTime) throws IOException;

  /**
   * List files that belong to the bucket of the key.
   * @param key
   * @return
   */
  List<BucketFileMeta> listFiles(DataKey key) throws IOException;
  DataOutputStream getOutputStream(DataKey key, BucketFileMeta name) throws IOException;
  DataOutputStream getDuplicatesOutputStream(DataKey key) throws IOException;
  DataInputStream getInputStream(DataKey key, BucketFileMeta name) throws IOException;

}

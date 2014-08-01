/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.hds.HDS.DataKey;

/**
 * Hadoop file system backed store.
 */
public class FSBucketFileSystem implements BucketFileSystem
{
  private final FileSystem fs;
  private final String basePath;

  public FSBucketFileSystem(FileSystem fs, String basePath)
  {
    this.fs = fs;
    this.basePath = basePath;
  }

  protected Path getBucketPath(long bucketKey)
  {
    return new Path(basePath, Long.toString(bucketKey));
  }

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public void init()
  {
  }

  @Override
  public void createFile(long bucketKey, BucketFileMeta fileMeta) throws IOException
  {
    // create empty file, override anything existing
    fs.create(new Path(getBucketPath(bucketKey), fileMeta.name), true).close();
  }

  @Override
  public DataOutputStream getOutputStream(DataKey key, String fileName) throws IOException
  {
    Path path = new Path(getBucketPath(key.getBucketKey()), fileName);
    if (!fs.exists(path)) {
      return fs.create(path);
    }
    return fs.append(path);
  }

  @Override
  public DataInputStream getInputStream(DataKey key, BucketFileMeta bfm) throws IOException
  {
    return fs.open(new Path(getBucketPath(key.getBucketKey()), bfm.name));
  }

}

/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.Rename;

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
  public void delete(long bucketKey, String fileName) throws IOException
  {
    fs.delete(new Path(getBucketPath(bucketKey), fileName), true);
  }

  @Override
  public DataOutputStream getOutputStream(long bucketKey, String fileName) throws IOException
  {
    Path path = new Path(getBucketPath(bucketKey), fileName);
    if (!fs.exists(path)) {
      return fs.create(path);
    }
    return fs.append(path);
  }

  @Override
  public DataInputStream getInputStream(long bucketKey, String fileName) throws IOException
  {
    return fs.open(new Path(getBucketPath(bucketKey), fileName));
  }

  @Override
  public void rename(long bucketKey, String fromName, String toName) throws IOException
  {
    FileContext fc = FileContext.getFileContext(fs.getUri());
    Path bucketPath = getBucketPath(bucketKey);
    fc.rename(new Path(bucketPath, fromName), new Path(bucketPath, toName), Rename.OVERWRITE);
  }

}

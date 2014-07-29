/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.python.google.common.collect.Lists;
import org.python.google.common.collect.Maps;
import org.python.google.common.collect.Sets;

import com.datatorrent.lib.hds.HDS.DataKey;

/**
 * Hadoop file system backed store.
 */
public class FSBucketFileSystem implements BucketFileSystem
{
  public static final String DUPLICATES = "_DUPLICATES";
  private final FileSystem fs;
  private final String basePath;

  private static class BucketMeta {
    int fileSeq;
    Set<BucketFileMeta> files = Sets.newHashSet();
  }

  private final HashMap<Long, BucketMeta> metaInfo = Maps.newHashMap();

  public FSBucketFileSystem(FileSystem fs, String basePath)
  {
    this.fs = fs;
    this.basePath = basePath;
  }

  protected Path getBucketPath(DataKey key)
  {
    return new Path(basePath, Long.toString(key.getBucketKey()));
  }

  @Override
  public BucketFileMeta createFile(DataKey key, long fromSeq, long toSeq) throws IOException
  {
    long bucketKey = key.getBucketKey();
    BucketMeta bm = metaInfo.get(bucketKey);
    if (bm == null) {
      // new bucket
      metaInfo.put(bucketKey, bm = new BucketMeta());
    }
    BucketFileMeta bfm = new BucketFileMeta();
    bfm.name = Long.toString(bucketKey) + '-' + bm.fileSeq++;
    bfm.fromSeq = fromSeq;
    bfm.toSeq = toSeq;
    bm.files.add(bfm);
    // create empty file, override anything existing
    fs.create(new Path(getBucketPath(key), bfm.name), true).close();
    return bfm;
  }

  @Override
  public List<BucketFileMeta> listFiles(DataKey key) throws IOException
  {
    List<BucketFileMeta> files = Lists.newArrayList();
    Long bucketKey = key.getBucketKey();
    BucketMeta bm = metaInfo.get(bucketKey);
    if (bm != null) {
      files.addAll(bm.files);
    }
    return files;
  }

  @Override
  public DataOutputStream getOutputStream(HDS.DataKey key, BucketFileMeta bfm) throws IOException
  {
    return fs.append(new Path(getBucketPath(key), bfm.name));
  }

  @Override
  public DataOutputStream getDuplicatesOutputStream(DataKey key) throws IOException
  {
    Path path = new Path(getBucketPath(key), DUPLICATES);
    if (!fs.exists(path)) {
      return fs.create(path);
    }
    return fs.append(new Path(getBucketPath(key), DUPLICATES));
  }

  @Override
  public DataInputStream getInputStream(DataKey key, BucketFileMeta bfm) throws IOException
  {
    return fs.open(new Path(getBucketPath(key), bfm.name));
  }

}

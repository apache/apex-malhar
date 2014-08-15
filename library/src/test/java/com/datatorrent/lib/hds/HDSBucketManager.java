/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.output.CountingOutputStream;
import org.python.google.common.collect.Sets;

import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.hds.BucketFileSystem.HDSFileReader;
import com.datatorrent.lib.hds.BucketFileSystem.HDSFileWriter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;


/**
 * Manager for buckets. Implements can be sub-classed as operator or used in composite pattern.
 */
public class HDSBucketManager implements HDS.BucketManager, CheckpointListener, Operator
{
  public static final String FNAME_WAL = "_WAL";
  public static final String FNAME_META = "_META";

  private final HashMap<Long, BucketMeta> metaCache = Maps.newHashMap();
  private transient long windowId;
  BucketFileSystem bfs;
  private final HashMap<Long, Bucket> buckets = Maps.newHashMap();
  private final transient Kryo kryo = new Kryo();

  @NotNull
  private Comparator<byte[]> keyComparator;
  private int maxFileSize = 64000;

  /**
   * Compare keys for sequencing as secondary level of organization within buckets.
   * In most cases it will be implemented using a time stamp as leading component.
   * @return
   */
  public Comparator<byte[]> getKeyComparator()
  {
    return this.keyComparator;
  }

  public void setKeyComparator(Comparator<byte[]> keyComparator)
  {
    this.keyComparator = keyComparator;
  }

  public int getMaxFileSize()
  {
    return maxFileSize;
  }

  public void setMaxFileSize(int maxFileSize)
  {
    this.maxFileSize = maxFileSize;
  }

  /**
   * Read entire file.
   * @param bucketKey
   * @param fileName
   * @return
   * @throws IOException
   */
  @VisibleForTesting
  protected TreeMap<byte[], byte[]> readFile(long bucketKey, String fileName) throws IOException
  {
    TreeMap<byte[], byte[]> data = Maps.newTreeMap(getKeyComparator());
    HDSFileReader reader = bfs.getReader(bucketKey, fileName);
    reader.readFully(data);
    reader.close();
    return data;
  }

  /**
   * Write data to size based rolling files
   * @param bucket
   * @param bucketMeta
   * @param data
   * @throws IOException
   */
  private void writeFile(Bucket bucket, BucketMeta bucketMeta, TreeMap<byte[], byte[]> data) throws IOException
  {
    HDSFileWriter fw = null;
    BucketFileMeta fileMeta = null;
    for (Map.Entry<byte[], byte[]> dataEntry : data.entrySet()) {
      if (fw == null) {
        // next file
        fileMeta = bucketMeta.addFile(bucket.bucketKey, dataEntry.getKey());
        fw = bfs.getWriter(bucket.bucketKey, fileMeta.name + ".tmp");
      }

      fw.append(dataEntry.getKey(), dataEntry.getValue());

      if (fw.getFileSize() > this.maxFileSize) {
        // roll file
        fw.close();
        bucket.fileDataCache.remove(fileMeta.name);
        bfs.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        fw = null;
      }
    }

    if (fw != null) {
      fw.close();
      bucket.fileDataCache.remove(fileMeta.name);
      bfs.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
    }

  }

  private Bucket getBucket(long bucketKey)
  {
    Bucket bucket = this.buckets.get(bucketKey);
    if (bucket == null) {
      bucket = new Bucket();
      bucket.bucketKey = bucketKey;
      this.buckets.put(bucketKey, bucket);
    }
    return bucket;
  }

  @Override
  public byte[] get(long bucketKey, byte[] key) throws IOException
  {
    Bucket bucket = getBucket(bucketKey);
    // check unwritten changes first
    byte[] v = bucket.writeCache.get(key);
    if (v != null) {
      return v;
    }

    BucketMeta bm = getMeta(bucketKey);
    if (bm == null) {
      throw new IllegalArgumentException("Invalid bucket key " + bucketKey);
    }

    Map.Entry<byte[], BucketFileMeta> floorEntry = bm.files.floorEntry(key);
    if (floorEntry == null) {
      // no file for this key
      return null;
    }

    // lookup against file data
    TreeMap<byte[], byte[]> fileData = bucket.fileDataCache.get(floorEntry.getValue().name);
    if (fileData == null) {
      // load file to check for presence of key
      fileData = readFile(bucketKey, floorEntry.getValue().name);
    }
    return fileData.get(key);
  }

  @Override
  public void put(long bucketKey, byte[] key, byte[] value) throws IOException
  {
    Bucket bucket = getBucket(bucketKey);
    if (bucket.wal == null) {
      bucket.wal = bfs.getOutputStream(bucket.bucketKey, FNAME_WAL);
      bucket.wal.write(key);
      bucket.wal.write(value);
    }
    bucket.writeCache.put(key, value);
  }

  /**
   * Flush changes from write cache to disk.
   * New data files will be written and meta data replaced atomically.
   * The flush frequency determines availability of changes to external readers.
   * @throws IOException
   */
  public void writeDataFiles() throws IOException
  {
    // bucket keys by file
    Map<Bucket, Map<BucketFileMeta, Map<byte[], byte[]>>> modifiedBuckets = Maps.newHashMap();

    for (Map.Entry<Long, Bucket> bucketEntry : this.buckets.entrySet()) {
      Bucket bucket = bucketEntry.getValue();
      BucketMeta bm = getMeta(bucketEntry.getKey());
      TreeMap<byte[], BucketFileMeta> bucketSeqStarts = bm.files;

      for (Map.Entry<byte[], byte[]> entry : bucket.writeCache.entrySet()) {

        // find the file for the key
        Map.Entry<byte[], BucketFileMeta> floorEntry = bucketSeqStarts.floorEntry(entry.getKey());
        BucketFileMeta floorFile;
        if (floorEntry != null) {
          floorFile = floorEntry.getValue();
        } else {
          floorEntry = bucketSeqStarts.firstEntry();
          if (floorEntry == null || floorEntry.getValue().name != null) {
            // no existing file or file with higher key
            floorFile = new BucketFileMeta();
          } else {
            // already have a placeholder for new keys, move start key
            floorFile = floorEntry.getValue();
          }
          floorFile.fromSeq = entry.getKey();
          bucketSeqStarts.put(floorFile.fromSeq, floorFile);
        }

        // add to existing file
        Map<BucketFileMeta, Map<byte[], byte[]>> modifiedFiles = modifiedBuckets.get(bucketEntry.getKey());
        if (modifiedFiles == null) {
          modifiedBuckets.put(bucketEntry.getValue(), modifiedFiles = Maps.newHashMap());
        }
        Map<byte[], byte[]> fileUpdates = modifiedFiles.get(floorFile);
        if (fileUpdates == null) {
          modifiedFiles.put(floorFile, fileUpdates = Maps.newHashMap());
        }
        fileUpdates.put(entry.getKey(), entry.getValue());

      }
    }

    // write modified files
    for (Map.Entry<Bucket, Map<BucketFileMeta, Map<byte[], byte[]>>> bucketEntry : modifiedBuckets.entrySet()) {

      Bucket bucket = bucketEntry.getKey();
      // copy on write
      BucketMeta bucketMetaCopy = kryo.copy(getMeta(bucket.bucketKey));
      HashSet<String> filesToDelete = Sets.newHashSet();

      for (Map.Entry<BucketFileMeta, Map<byte[], byte[]>> fileEntry : bucketEntry.getValue().entrySet()) {
        BucketFileMeta fileMeta = fileEntry.getKey();
        TreeMap<byte[], byte[]> fileData;

        if (fileMeta.name != null) {
          // load existing file
          fileData = bucket.fileDataCache.get(fileMeta.name);
          if (fileData == null) {
            fileData = readFile(bucket.bucketKey, fileMeta.name);
          }
          filesToDelete.add(fileMeta.name);
        } else {
          fileData = Maps.newTreeMap(getKeyComparator());
        }

        // apply updates
        fileData.putAll(fileEntry.getValue());
        // new file
        writeFile(bucket, bucketMetaCopy, fileData);
      }

      // flush meta data for new files
      try {
        OutputStream os = bfs.getOutputStream(bucket.bucketKey, FNAME_META + ".new");
        Output output = new Output(os);
        kryo.writeClassAndObject(output, bucketMetaCopy);
        output.close();
        os.close();
        bfs.rename(bucket.bucketKey, FNAME_META + ".new", FNAME_META);
      } catch (IOException e) {
        throw new RuntimeException("Failed to write bucket meta data " + bucket.bucketKey, e);
      }

      // clear pending changes
      bucket.writeCache.clear();
      // switch to new version
      this.metaCache.put(bucket.bucketKey, bucketMetaCopy);

      // delete old files
      for (String fileName : filesToDelete) {
        bfs.delete(bucket.bucketKey, fileName);
        bucket.fileDataCache.remove(fileName);
      }

    }

  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    for (Bucket bucket : this.buckets.values()) {
      try {
        if (bucket.wal != null)
          bucket.wal.flush();
      } catch (IOException e) {
        throw new RuntimeException("Failed to flush WAL", e);
      }
    }
  }

  @Override
  public void checkpointed(long arg0)
  {
  }

  /**
   * Get meta data from cache or load it on first access
   * @param bucketKey
   * @return
   */
  private BucketMeta getMeta(long bucketKey)
  {
    BucketMeta bm = metaCache.get(bucketKey);
    if (bm == null) {
      bm = loadBucketMeta(bucketKey);
      metaCache.put(bucketKey, bm);
    }
    return bm;
  }

  private BucketMeta loadBucketMeta(long bucketKey)
  {
    BucketMeta bucketMeta = null;
    try {
      InputStream is = bfs.getInputStream(bucketKey, FNAME_META);
      bucketMeta = (BucketMeta)kryo.readClassAndObject(new Input(is));
      is.close();
    } catch (IOException e) {
      bucketMeta = new BucketMeta(getKeyComparator());
    }
    return bucketMeta;
  }

  @Override
  public void committed(long committedWindowId)
  {
  }

  public static class BucketFileMeta
  {
    /**
     * Name of file (relative to bucket)
     */
    public String name;
    /**
     * Lower bound sequence key
     */
    public byte[] fromSeq;

    @Override
    public String toString()
    {
      return "BucketFileMeta [name=" + name + ", fromSeq=" + Arrays.toString(fromSeq) + "]";
    }
  }

  /**
   * Meta data about bucket, persisted in store
   * Flushed on compaction
   */
  private static class BucketMeta
  {
    private BucketMeta(Comparator<byte[]> cmp)
    {
      files = Maps.newTreeMap(cmp);
    }

    private BucketMeta()
    {
      // for serialization only
      files = null;
    }

    private BucketFileMeta addFile(long bucketKey, byte[] fromSeq)
    {
      BucketFileMeta bfm = new BucketFileMeta();
      bfm.name = Long.toString(bucketKey) + '-' + this.fileSeq++;
      bfm.fromSeq = fromSeq;
      files.put(fromSeq, bfm);
      return bfm;
    }

    int fileSeq;
    final TreeMap<byte[], BucketFileMeta> files;
  }

  private static class Bucket
  {
    private long bucketKey;
    // keys that were modified and written to WAL, but not yet persisted
    private final HashMap<byte[], byte[]> writeCache = Maps.newHashMap();
    private transient DataOutputStream wal;
    private final transient HashMap<String, TreeMap<byte[], byte[]>> fileDataCache = Maps.newHashMap();
  }

}

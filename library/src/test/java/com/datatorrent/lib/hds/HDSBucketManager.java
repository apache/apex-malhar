/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.output.CountingOutputStream;

import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.hds.BucketFileSystem.BucketFileMeta;
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

  // TODO: leave serialization to API client
  private final Kryo writeSerde = new Kryo();

  @NotNull
  private Comparator<byte[]> keyComparator;
  private int maxFileSize = 64000;

  /**
   * Compare keys for sequencing as secondary level of organization within buckets.
   * In most cases this will be implemented using a time stamp as leading component.
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

  private BucketFileMeta createFile(long bucketKey, byte[] fromSeq) throws IOException
  {
    BucketMeta bm = getMeta(bucketKey);
    BucketFileMeta bfm = new BucketFileMeta();
    bfm.name = Long.toString(bucketKey) + '-' + bm.fileSeq++;
    bfm.fromSeq = fromSeq;
    bm.files.put(fromSeq, bfm);
    // initialize with empty file
    //bfs.createFile(bucketKey, bfm);
    return bfm;
  }

  /**
   * Read entire file.
   * TODO: delegate to the file reader
   * @param bucketKey
   * @param bfm
   * @return
   * @throws IOException
   */
  @VisibleForTesting
  protected TreeMap<byte[], byte[]> readFile(long bucketKey, String fileName) throws IOException
  {
    TreeMap<byte[], byte[]> data = Maps.newTreeMap(getKeyComparator());
    InputStream is = bfs.getInputStream(bucketKey, fileName);
    Input input = new Input(is);
    while (!input.eof()) {
      byte[] key = writeSerde.readObject(input, byte[].class);
      byte[] value = writeSerde.readObject(input, byte[].class);
      data.put(key, value);
    }
    return data;
  }

  /**
   * Write (replace) the bucket file.
   * TODO: delegate to the file writer
   * @param bucketKey
   * @param fileMeta
   * @param data
   */
  private void writeFile(Bucket bucket, BucketFileMeta fileMeta, TreeMap<byte[], byte[]> data) throws IOException
  {
    DataOutputStream dos = null;
    CountingOutputStream cos = null;
    Output out = null;
    for (Map.Entry<byte[], byte[]> dataEntry : data.entrySet()) {
      if (out == null) {
        if (fileMeta == null) {
          // roll file
          fileMeta = createFile(bucket.bucketKey, dataEntry.getKey());
        }
        dos = bfs.getOutputStream(bucket.bucketKey, fileMeta.name + ".tmp");
        cos = new CountingOutputStream(dos);
        out = new Output(cos);
      }

      writeSerde.writeObject(out, dataEntry.getKey());
      writeSerde.writeObject(out, dataEntry.getValue());

      if (cos.getCount() + out.position() > this.maxFileSize) {
        // roll file
        out.close();
        cos.close();
        dos.close();
        bucket.fileDataCache.remove(fileMeta.name);
        bfs.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        fileMeta = null;
        out = null;
      }
    }

    if (out != null) {
      out.close();
      cos.close();
      dos.close();
      bfs.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
    }

    // track meta data change by window
    LinkedHashMap<Long, BucketFileMeta> bucketMetaChanges = bucket.metaUpdates;
    bucketMetaChanges.put(windowId, writeSerde.copy(fileMeta));
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
    byte[] v = bucket.uncommittedChanges.get(key);
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
    bucket.uncommittedChanges.put(key, value);
  }

  public void writeDataFiles() throws IOException
  {
    // bucket keys by file
    Map<Bucket, Map<BucketFileMeta, Map<byte[], byte[]>>> modifiedBuckets = Maps.newHashMap();

    for (Map.Entry<Long, Bucket> bucketEntry : this.buckets.entrySet()) {
      Bucket bucket = bucketEntry.getValue();

      for (Map.Entry<byte[], byte[]> entry : bucket.uncommittedChanges.entrySet()) {

        BucketMeta bm = getMeta(bucketEntry.getKey());
        TreeMap<byte[], BucketFileMeta> bucketSeqStarts = bm.files;

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
      for (Map.Entry<BucketFileMeta, Map<byte[], byte[]>> fileEntry : bucketEntry.getValue().entrySet()) {
        BucketFileMeta fileMeta = fileEntry.getKey();
        TreeMap<byte[], byte[]> fileData;
        if (fileMeta.name == null) {
          // new file
          fileMeta = createFile(bucketEntry.getKey().bucketKey, fileMeta.fromSeq);
          fileData = Maps.newTreeMap(getKeyComparator());
        } else {
          // existing file
          fileData = bucket.fileDataCache.get(fileMeta.name);
          if (fileData == null) {
            // load file to check for presence of key
            fileData = readFile(bucket.bucketKey, fileMeta.name);
          }
        }

        // apply updates
        fileData.putAll(fileEntry.getValue());
        writeFile(bucket, fileMeta, fileData);
        bucket.uncommittedChanges.clear();

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
      bucketMeta = (BucketMeta)writeSerde.readClassAndObject(new Input(is));
      is.close();
    } catch (IOException e) {
      bucketMeta = new BucketMeta(getKeyComparator());
    }
    return bucketMeta;
  }

  @Override
  public void committed(long committedWindowId)
  {
    for (Bucket bucket : this.buckets.values())
    {
      // update meta data for modified files
      BucketMeta bucketMeta = loadBucketMeta(bucket.bucketKey);
      Map<Long, BucketFileMeta> files = bucket.metaUpdates;
      for (long windowId : files.keySet()) {
        if (windowId <= committedWindowId) {
          BucketFileMeta bfm = files.get(windowId);
          bucketMeta.files.put(bfm.fromSeq, bfm);
        } else {
          break;
        }
      }

      try {
        OutputStream os = bfs.getOutputStream(bucket.bucketKey, FNAME_META + ".new");
        Output output = new Output(os);
        writeSerde.writeClassAndObject(output, bucketMeta);
        output.close();
        bfs.rename(bucket.bucketKey, FNAME_META + ".new", FNAME_META);
      } catch (IOException e) {
        throw new RuntimeException("Failed to write bucket meta data " + bucket.bucketKey, e);
      }
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

    int fileSeq;
    final TreeMap<byte[], BucketFileMeta> files;
  }

  private static class Bucket
  {
    private long bucketKey;
    private transient DataOutputStream wal;
    // keys that were modified and written to WAL, but not yet persisted
    private final HashMap<byte[], byte[]> uncommittedChanges = Maps.newHashMap();
    // change log by bucket by window
    private final LinkedHashMap<Long, BucketFileMeta> metaUpdates = Maps.newLinkedHashMap();
    private final transient HashMap<String, TreeMap<byte[], byte[]>> fileDataCache = Maps.newHashMap();
  }

}

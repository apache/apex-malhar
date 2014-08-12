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
 *
 */
public class HDSPrototype<K extends HDS.DataKey, V> implements HDS.Bucket<K, V>, CheckpointListener, Operator
{
  /*
   * Meta data about bucket, persisted in store
   * Modified on compaction
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
      throw new RuntimeException();
    }

    int fileSeq;
    final TreeMap<byte[], BucketFileMeta> files;
  }

  public static final String FNAME_WAL = "_WAL";
  public static final String FNAME_META = "_META";

  private final HashMap<Long, BucketMeta> metaCache = Maps.newHashMap();
  // change log by bucket by window
  private final HashMap<Long, LinkedHashMap<Long, BucketFileMeta>> metaUpdates = Maps.newLinkedHashMap();
  private transient HashMap<Long, DataOutputStream> walMap = Maps.newHashMap();
  private transient long windowId;

  BucketFileSystem bfs;

  // keys that were modified and written to WAL, but not yet persisted
  private final HashMap<K, Map.Entry<K, V>> uncommittedChanges = Maps.newHashMap();
  private final HashMap<String, TreeMap<byte[], V>> cache = Maps.newHashMap();
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
  protected TreeMap<byte[], V> readFile(long bucketKey, String fileName) throws IOException
  {
    TreeMap<byte[], V> data = Maps.newTreeMap(getKeyComparator());
    InputStream is = bfs.getInputStream(bucketKey, fileName);
    Input input = new Input(is);
    while (!input.eof()) {
      byte[] key = writeSerde.readObject(input, byte[].class);
      @SuppressWarnings("unchecked")
      V value = (V)writeSerde.readClassAndObject(input);
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
  private void writeFile(long bucketKey, BucketFileMeta fileMeta, TreeMap<byte[], V> data) throws IOException
  {
    DataOutputStream dos = null;
    CountingOutputStream cos = null;
    Output out = null;
    for (Map.Entry<byte[], V> dataEntry : data.entrySet()) {
      if (out == null) {
        if (fileMeta == null) {
          // roll file
          fileMeta = createFile(bucketKey, dataEntry.getKey());
        }
        dos = bfs.getOutputStream(bucketKey, fileMeta.name + ".tmp");
        cos = new CountingOutputStream(dos);
        out = new Output(cos);
      }

      writeSerde.writeObject(out, dataEntry.getKey());
      writeSerde.writeClassAndObject(out, dataEntry.getValue());

      if (cos.getCount() + out.position() > this.maxFileSize) {
        // roll file
        out.close();
        cos.close();
        dos.close();
        this.cache.remove(fileMeta.name);
        bfs.rename(bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        fileMeta = null;
        out = null;
      }
    }

    if (out != null) {
      out.close();
      cos.close();
      dos.close();
      bfs.rename(bucketKey, fileMeta.name + ".tmp", fileMeta.name);
    }

    // track meta data change by window
    LinkedHashMap<Long, BucketFileMeta> bucketMetaChanges = this.metaUpdates.get(bucketKey);
    if (bucketMetaChanges == null) {
      this.metaUpdates.put(bucketKey, bucketMetaChanges = Maps.newLinkedHashMap());
    }
    bucketMetaChanges.put(windowId, writeSerde.copy(fileMeta));
  }

  @Override
  public void put(Map.Entry<K, V> entry) throws IOException
  {
    DataOutputStream wal = walMap.get(entry.getKey().getBucketKey());
    if (wal == null) {
      wal = bfs.getOutputStream(entry.getKey().getBucketKey(), FNAME_WAL);
      walMap.put(entry.getKey().getBucketKey(), wal);
    }
    this.uncommittedChanges.put(entry.getKey(), entry);
  }

  public void writeDataFiles() throws IOException
  {
    // bucket keys by file
    Map<Long, Map<BucketFileMeta, Map<byte[], V>>> modifiedBuckets = Maps.newHashMap();

    for (Map.Entry<K, Map.Entry<K, V>> entry : uncommittedChanges.entrySet()) {
      //byte[] key = entry.getKey().getBytes();
      // find files to check for existing key
      long bucketKey = entry.getKey().getBucketKey();
      BucketMeta bm = getMeta(bucketKey);
      TreeMap<byte[], BucketFileMeta> bucketSeqStarts = bm.files;

      // find the file for the key
      Map.Entry<byte[], BucketFileMeta> floorEntry = bucketSeqStarts.floorEntry(entry.getKey().getBytes());
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
        floorFile.fromSeq = entry.getKey().getBytes();
        bucketSeqStarts.put(floorFile.fromSeq, floorFile);
      }

      // add to existing file
      Map<BucketFileMeta, Map<byte[], V>> modifiedFiles = modifiedBuckets.get(bucketKey);
      if (modifiedFiles == null) {
        modifiedBuckets.put(bucketKey, modifiedFiles = Maps.newHashMap());
      }
      Map<byte[], V> fileUpdates = modifiedFiles.get(floorFile);
      if (fileUpdates == null) {
        modifiedFiles.put(floorFile, fileUpdates = Maps.newHashMap());
      }
      fileUpdates.put(entry.getKey().getBytes(), entry.getValue().getValue());

    }

    // write modified files
    for (Map.Entry<Long, Map<BucketFileMeta, Map<byte[], V>>> bucketEntry : modifiedBuckets.entrySet()) {
      for (Map.Entry<BucketFileMeta, Map<byte[], V>> fileEntry : bucketEntry.getValue().entrySet()) {
        BucketFileMeta fileMeta = fileEntry.getKey();
        TreeMap<byte[], V> fileData;
        if (fileMeta.name == null) {
          // new file
          fileMeta = createFile(bucketEntry.getKey(), fileMeta.fromSeq);
          fileData = Maps.newTreeMap(getKeyComparator());
        } else {
          // existing file
          fileData = cache.get(fileMeta.name);
          if (fileData == null) {
            // load file to check for presence of key
            fileData = readFile(bucketEntry.getKey(), fileMeta.name);
          }
        }

        // apply updates
        fileData.putAll(fileEntry.getValue());
        writeFile(bucketEntry.getKey(), fileMeta, fileData);

      }
    }

    uncommittedChanges.clear();
  }

  @Override
  public V get(K key) throws IOException
  {
    // check unwritten changes first
    Map.Entry<K, V> kv = uncommittedChanges.get(key);
    if (kv != null) {
      return kv.getValue();
    }

    BucketMeta bm = getMeta(key.getBucketKey());
    if (bm == null) {
      throw new IllegalArgumentException("Invalid bucket key " + key.getBucketKey());
    }

    Map.Entry<byte[], BucketFileMeta> floorEntry = bm.files.floorEntry(key.getBytes());
    if (floorEntry == null) {
      // no file for this key
      return null;
    }

    // lookup against file data
    TreeMap<byte[], V> fileData = cache.get(floorEntry.getValue().name);
    if (fileData == null) {
      // load file to check for presence of key
      fileData = readFile(key.getBucketKey(), floorEntry.getValue().name);
    }
    return fileData.get(key);
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
    for (DataOutputStream wal : walMap.values()) {
      try {
        wal.flush();
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
    for (long bucketKey : this.metaUpdates.keySet())
    {
      // update meta data for modified files
      BucketMeta bucketMeta = loadBucketMeta(bucketKey);
      Map<Long, BucketFileMeta> files = this.metaUpdates.get(bucketKey);
      for (long windowId : files.keySet()) {
        if (windowId <= committedWindowId) {
          BucketFileMeta bfm = files.get(windowId);
          bucketMeta.files.put(bfm.fromSeq, bfm);
        } else {
          break;
        }
      }

      try {
        OutputStream os = bfs.getOutputStream(bucketKey, FNAME_META + ".new");
        Output output = new Output(os);
        writeSerde.writeClassAndObject(output, bucketMeta);
        output.close();
        bfs.rename(bucketKey, FNAME_META + ".new", FNAME_META);
      } catch (IOException e) {
        throw new RuntimeException("Failed to write bucket meta data " + bucketKey, e);
      }
    }
  }


}

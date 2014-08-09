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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.Range;

import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.hds.BucketFileSystem.BucketFileMeta;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


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
    int fileSeq;
    Set<BucketFileMeta> files = Sets.newHashSet();
  }

  public static final String FNAME_WAL = "_WAL";
  public static final String FNAME_META = "_META";

  private final HashMap<Long, BucketMeta> metaCache = Maps.newHashMap();
  // change log by bucket by window
  private final HashMap<Long, LinkedHashMap<Long, BucketFileMeta>> metaUpdates = Maps.newLinkedHashMap();
  private transient HashMap<Long, DataOutputStream> walMap = Maps.newHashMap();
  private transient long windowId;

  BucketFileSystem bfs;
  int maxSize;
  // second level of partitioning by time
  TimeUnit timeBucketUnit = TimeUnit.HOURS;
  int timeBucketSize = 1;

  // keys that were modified and written to WAL, but not yet persisted
  private final HashMap<K, Map.Entry<K, V>> uncommittedChanges = Maps.newHashMap();
  private final HashMap<String, TreeMap<byte[], V>> cache = Maps.newHashMap();
  private final Kryo writeSerde = new Kryo();

  @NotNull
  private Comparator<byte[]> keyComparator;

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

  protected Range<Long> getRange(long time)
  {
    long timeBucket = this.timeBucketUnit.convert(time, TimeUnit.MILLISECONDS);
    timeBucket = timeBucket - (timeBucket % this.timeBucketSize);
    long min = TimeUnit.MILLISECONDS.convert(timeBucket, timeBucketUnit);
    long max = TimeUnit.MILLISECONDS.convert(timeBucket + timeBucketSize, timeBucketUnit);
    return Range.between(min, max);
  }

  private BucketFileMeta createFile(long bucketKey, byte[] fromSeq) throws IOException
  {
    BucketMeta bm = getMeta(bucketKey);
    BucketFileMeta bfm = new BucketFileMeta();
    bfm.name = Long.toString(bucketKey) + '-' + bm.fileSeq++;
    bfm.fromSeq = fromSeq;
    bm.files.add(bfm);
    // create empty file, override anything existing
    bfs.createFile(bucketKey, bfm);
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
  private TreeMap<byte[], V> readFile(long bucketKey, BucketFileMeta bfm) throws IOException
  {
    TreeMap<byte[], V> data = Maps.newTreeMap(getKeyComparator());
    InputStream is = bfs.getInputStream(bucketKey, bfm.name);
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
    DataOutputStream dos = bfs.getOutputStream(bucketKey, fileMeta.name + ".tmp");
    Output out = new Output(dos);
    for (Map.Entry<byte[], V> dataEntry : data.entrySet()) {
      writeSerde.writeObject(out, dataEntry.getKey());
      writeSerde.writeClassAndObject(out, dataEntry.getValue());
    }
    out.flush();
    dos.close();
    bfs.rename(bucketKey, fileMeta.name + ".tmp", fileMeta.name);

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
    // TODO: WAL
    // bfs.getOutputStream(entry.getKey().getBucketKey(), FNAME_WAL);
    this.uncommittedChanges.put(entry.getKey(), entry);
  }

  public void writeDataFiles() throws IOException
  {
    // map to lookup the range for each key
    Map<BucketMeta, TreeMap<byte[], BucketFileMeta>> sequenceRanges = Maps.newHashMap();
    // bucket keys by file
    Map<Long, Map<BucketFileMeta, Map<byte[], V>>> modifiedBuckets = Maps.newHashMap();

    for (Map.Entry<K, Map.Entry<K, V>> entry : uncommittedChanges.entrySet()) {
      //byte[] key = entry.getKey().getBytes();
      // find files to check for existing key
      long bucketKey = entry.getKey().getBucketKey();
      BucketMeta bm = getMeta(bucketKey);
      TreeMap<byte[], BucketFileMeta> bucketSeqStarts = sequenceRanges.get(bm);
      if (bm == null) {
        // load sequence ranges from file meta
        sequenceRanges.put(bm, bucketSeqStarts = Maps.newTreeMap(getKeyComparator()));
        for (BucketFileMeta bfm : bm.files) {
          bucketSeqStarts.put(bfm.fromSeq, bfm);
        }
      }

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
        modifiedFiles.put(floorEntry.getValue(), fileUpdates = Maps.newHashMap());
      }
      fileUpdates.put(entry.getKey().getBytes(), entry.getValue().getValue());

    }

    // write out new and modified files
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
            fileData = readFile(bucketEntry.getKey(), fileMeta);
          }
        }

        // apply updates
        fileData.putAll(fileEntry.getValue());
        writeFile(bucketEntry.getKey(), fileMeta, fileData);
      }
    }
  }


  @Override
  public V get(K key) throws IOException
  {
    Set<BucketFileMeta> files = getMeta(key.getBucketKey()).files;
    for (BucketFileMeta bfm : files) {
      TreeMap<byte[], V> data = cache.get(bfm.name);
      if (data == null) {
        // load file to check for presence of key
        cache.put(bfm.name, data = readFile(key.getBucketKey(), bfm));
      }
      if (data.containsKey(key)) {
        return data.get(key);
      }
    }
    return null;
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
    // TODO Auto-generated method stub

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
      bucketMeta = new BucketMeta();
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
          bucketMeta.files.add(files.get(windowId));
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

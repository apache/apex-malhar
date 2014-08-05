/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Range;

import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.hds.BucketFileSystem.BucketFileMeta;
import com.datatorrent.lib.hds.HDS.DataKey;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 *
 */
public class HDSPrototype<K extends HDS.DataKey, V> implements HDS.Bucket<K, V>, CheckpointListener, Operator
{
  private static class BucketMeta {
    int fileSeq;
    Set<BucketFileMeta> files = Sets.newHashSet();
  }

  public static final String FNAME_WAL = "_WAL";
  public static final String FNAME_META = "_META";

  private final transient HashMap<Long, BucketMeta> metaInfo = Maps.newHashMap();
  // change log by bucket by window
  private final HashMap<Long, LinkedHashMap<Long, BucketFileMeta>> metaUpdates = Maps.newLinkedHashMap();
  private transient long windowId;

  BucketFileSystem bfs;
  int maxSize;
  // second level of partitioning by time
  TimeUnit timeBucketUnit = TimeUnit.HOURS;
  int timeBucketSize = 1;

  // TODO: managed cache
  private final HashMap<String, HashMap<K, V>> cache = Maps.newHashMap();
  private final Kryo writeSerde = new Kryo();

  protected Range<Long> getRange(long time)
  {
    long timeBucket = this.timeBucketUnit.convert(time, TimeUnit.MILLISECONDS);
    timeBucket = timeBucket - (timeBucket % this.timeBucketSize);
    long min = TimeUnit.MILLISECONDS.convert(timeBucket, timeBucketUnit);
    long max = TimeUnit.MILLISECONDS.convert(timeBucket + timeBucketSize, timeBucketUnit);
    return Range.between(min, max);
  }

  private byte[] toBytes(Map.Entry<K, V> data)
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output out = new Output(bos);
    writeSerde.writeClassAndObject(out, data);
    out.flush();
    return bos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private Map.Entry<K, V> fromBytes(byte[] buffer) throws IOException
  {
    return (Map.Entry<K, V>)writeSerde.readClassAndObject(new Input(buffer));
  }

  private BucketFileMeta createFile(DataKey key, long fromSeq, long toSeq) throws IOException
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
    bfs.createFile(bucketKey, bfm);
    return bfm;
  }

  private HashMap<K, V> loadFile(DataKey key, BucketFileMeta bfm) throws IOException
  {
    HashMap<K, V> data = Maps.newHashMap();
    InputStream is = bfs.getInputStream(key.getBucketKey(), bfm.name);
    DataInputStream dis = new DataInputStream(is);

    int pos = 0;
    while (pos < bfm.committedOffset) {
      try {
        int len = dis.readInt();
        pos += 4;
        byte[] buffer = new byte[len];
        pos += dis.read(buffer);
        Map.Entry<K, V> entry = fromBytes(buffer);
        data.put(entry.getKey(), entry.getValue());
      }
      catch (EOFException e) {
        break;
      }
    }
    return data;
  }

  @Override
  public void put(Map.Entry<K, V> entry) throws IOException
  {
    byte[] bytes = toBytes(entry);
    long sequence = entry.getKey().getSequenceKey();

    List<BucketFileMeta> files = listFiles(entry.getKey());
    ArrayList<BucketFileMeta> bucketFiles = Lists.newArrayList();
    // find files to check for existing key
    for (BucketFileMeta bfm : files) {
      if (sequence == 0 || bfm.fromSeq <= sequence && bfm.toSeq > sequence) {
        bucketFiles.add(bfm);
      }
    }

    boolean duplicateKey = false;
    BucketFileMeta targetFile = null;

    // check for existing key / find file with head room
    for (BucketFileMeta bfm : bucketFiles) {
      HashMap<K, V> data = cache.get(bfm.name);
      if (data == null) {
        // load file to check for presence of key
        cache.put(bfm.name, data = loadFile(entry.getKey(), bfm));
      }
      if (data.containsKey(entry.getKey())) {
        // key exists, add to duplicates
        duplicateKey = true;
        break;
      }
      if (bfm.committedOffset + bytes.length < maxSize) {
        targetFile = bfm;
      }
    }

    DataOutputStream dos;
    if (duplicateKey) {
      // append to duplicates file
      dos = bfs.getOutputStream(entry.getKey().getBucketKey(), FNAME_WAL);
    } else {
      if (targetFile == null) {
        // create new file
        Range<Long> r  = getRange(sequence);
        targetFile = createFile(entry.getKey(), r.getMinimum(), r.getMaximum());
      }
      // append to existing file
      dos = bfs.getOutputStream(entry.getKey().getBucketKey(), targetFile.name);
      targetFile.committedOffset += (4+bytes.length);
      // track meta data change by window
      LinkedHashMap<Long, BucketFileMeta> bucketMetaChanges = this.metaUpdates.get(entry.getKey().getBucketKey());
      if (bucketMetaChanges == null) {
        this.metaUpdates.put(entry.getKey().getBucketKey(), bucketMetaChanges = Maps.newLinkedHashMap());
      }
      bucketMetaChanges.put(windowId, writeSerde.copy(targetFile));
    }
    // TODO: batching
    dos.writeInt(bytes.length);
    dos.write(bytes);
    dos.close();

  }

  @Override
  public V get(K key) throws IOException
  {
    List<BucketFileMeta> files = listFiles(key);
    for (BucketFileMeta bfm : files) {
      HashMap<K, V> data = cache.get(bfm.name);
      if (data == null) {
        // load file to check for presence of key
        cache.put(bfm.name, data = loadFile(key, bfm));
      }
      if (data.containsKey(key)) {
        return data.get(key);
      }
    }
    return null;
  }

  private List<BucketFileMeta> listFiles(DataKey key) throws IOException
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

  @Override
  public void committed(long committedWindowId)
  {
    for (long bucketKey : this.metaUpdates.keySet())
    {
      BucketMeta bucketMeta;
      try {
        InputStream is = bfs.getInputStream(bucketKey, FNAME_META);
        bucketMeta = (BucketMeta)writeSerde.readClassAndObject(new Input(is));
        is.close();
      } catch (IOException e) {
        bucketMeta = new BucketMeta();
      }

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

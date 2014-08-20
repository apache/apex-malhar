/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hds;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileReader;
import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileWriter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * Manager for buckets. Can be sub-classed as operator or used in composite pattern.
 */
public class HDSBucketManager implements HDS.BucketManager, CheckpointListener, Operator
{
  public static final String FNAME_WAL = "_WAL";
  public static final String FNAME_META = "_META";

  private final HashMap<Long, BucketMeta> metaCache = Maps.newHashMap();
  private transient long windowId;
  private final HashMap<Long, Bucket> buckets = Maps.newHashMap();
  private final transient Kryo kryo = new Kryo();
  @VisibleForTesting
  protected transient ExecutorService writeExecutor;
  private transient Exception writerError;

  @NotNull
  private Comparator<byte[]> keyComparator;
  @Valid
  @NotNull
  private HDSFileAccess fileStore;
  private int maxFileSize = 64000;
  private int flushSize = 100;


  public HDSFileAccess getFileStore()
  {
    return fileStore;
  }

  public void setFileStore(HDSFileAccess fileStore)
  {
    this.fileStore = fileStore;
  }

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

  /**
   * Size limit for data files. Files are rolled once the limit has been exceeded.
   * The final size of a file can be larger than the limit by the size of the last/single entry written to it.
   * @return
   */
  public int getMaxFileSize()
  {
    return maxFileSize;
  }

  public void setMaxFileSize(int maxFileSize)
  {
    this.maxFileSize = maxFileSize;
  }

  /**
   * The number of changes collected in memory before flushing to persistent storage.
   * @return
   */
  public int getFlushSize()
  {
    return flushSize;
  }

  public void setFlushSize(int flushSize)
  {
    this.flushSize = flushSize;
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
    HDSFileReader reader = fileStore.getReader(bucketKey, fileName);
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
        fw = fileStore.getWriter(bucket.bucketKey, fileMeta.name + ".tmp");
      }

      fw.append(dataEntry.getKey(), dataEntry.getValue());

      if (fw.getFileSize() > this.maxFileSize) {
        // roll file
        fw.close();
        bucket.fileDataCache.remove(fileMeta.name);
        fileStore.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        fw = null;
      }
    }

    if (fw != null) {
      fw.close();
      bucket.fileDataCache.remove(fileMeta.name);
      fileStore.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
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

    // check changes currently being flushed
    v = bucket.frozenWriteCache.get(key);
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
      bucket.wal = fileStore.getOutputStream(bucket.bucketKey, FNAME_WAL);
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
  private void writeDataFiles() throws IOException
  {
    // bucket keys by file
    Map<Bucket, Map<BucketFileMeta, Map<byte[], byte[]>>> modifiedBuckets = Maps.newHashMap();

    for (Map.Entry<Long, Bucket> bucketEntry : this.buckets.entrySet()) {
      Bucket bucket = bucketEntry.getValue();
      BucketMeta bm = getMeta(bucketEntry.getKey());
      TreeMap<byte[], BucketFileMeta> bucketSeqStarts = bm.files;

      for (Map.Entry<byte[], byte[]> entry : bucket.frozenWriteCache.entrySet()) {

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
        OutputStream os = fileStore.getOutputStream(bucket.bucketKey, FNAME_META + ".new");
        Output output = new Output(os);
        kryo.writeClassAndObject(output, bucketMetaCopy);
        output.close();
        os.close();
        fileStore.rename(bucket.bucketKey, FNAME_META + ".new", FNAME_META);
      } catch (IOException e) {
        throw new RuntimeException("Failed to write bucket meta data " + bucket.bucketKey, e);
      }

      // clear pending changes
      bucket.frozenWriteCache.clear();
      // switch to new version
      this.metaCache.put(bucket.bucketKey, bucketMetaCopy);

      // delete old files
      for (String fileName : filesToDelete) {
        fileStore.delete(bucket.bucketKey, fileName);
        bucket.fileDataCache.remove(fileName);
      }

    }

  }

  @Override
  public void setup(OperatorContext arg0)
  {
    fileStore.init();
    writeExecutor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(this.getClass().getSimpleName()+"-Writer"));
  }

  @Override
  public void teardown()
  {
    try {
      fileStore.close();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    writeExecutor.shutdown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    boolean scheduleFlush = false;
    for (Bucket bucket : this.buckets.values()) {
      try {
        if (bucket.wal != null)
          bucket.wal.flush();
      } catch (IOException e) {
        throw new RuntimeException("Failed to flush WAL", e);
      }

      if (bucket.writeCache.size() > this.flushSize && !bucket.writeCache.isEmpty()) {
        // ensure previous flush completed
        if (bucket.frozenWriteCache.isEmpty()) {
          bucket.frozenWriteCache = bucket.writeCache;
          bucket.writeCache = Maps.newHashMap();
          scheduleFlush = true;
        }
      }
    }

    if (scheduleFlush) {
      Runnable flushRunnable = new Runnable() {
        @Override
        public void run()
        {
          try {
            writeDataFiles();
          } catch (IOException e) {
            writerError = e;
          }
        }
      };
      this.writeExecutor.execute(flushRunnable);

      if (writerError != null) {
        throw new RuntimeException("Error while flushing write cache.", this.writerError);
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
      InputStream is = fileStore.getInputStream(bucketKey, FNAME_META);
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
    private HashMap<byte[], byte[]> writeCache = Maps.newHashMap();
    private HashMap<byte[], byte[]> frozenWriteCache = Maps.newHashMap();
    private transient DataOutputStream wal;
    private final transient HashMap<String, TreeMap<byte[], byte[]>> fileDataCache = Maps.newHashMap();
  }

}

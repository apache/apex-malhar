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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.constraints.Min;

import com.datatorrent.api.Context;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileReader;
import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileWriter;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Writes data to buckets. Can be sub-classed as operator or used in composite pattern.
 * <p>
 * Changes are accumulated in a write cache and written to a write-ahead-log (WAL). They are then asynchronously flushed
 * to the data files when thresholds for the memory buffer are reached.
 * <p>
 * When data is read through the same operator (extends reader), full consistency is guaranteed (reads will consider
 * changes that are not flushed). In the event of failure, the operator recovers the write buffer from the WAL.
 * <p>
 * Note that currently changes are not flushed at a committed window boundary, hence uncommitted changes may be read
 * from data files after recovery, making the operator non-idempotent.
 *
 * @displayName HDS Writer
 * @category Output
 * @tags hds, output operator
 */

public class HDSWriter extends HDSReader implements CheckpointListener, Operator, HDS.Writer
{

  private final transient HashMap<Long, BucketMeta> metaCache = Maps.newHashMap();
  private long windowId;
  private transient long lastFlushWindowId;
  private final transient HashMap<Long, Bucket> buckets = Maps.newHashMap();
  @VisibleForTesting
  protected transient ExecutorService writeExecutor;
  private volatile transient Throwable writerError;

  private int maxFileSize = 128 * 1024 * 1024; // 128m
  private int maxWalFileSize = 64 * 1024 * 1024;
  private int flushSize = 1000000;
  private int flushIntervalCount = 120;

  private final HashMap<Long, WalMeta> walMeta = Maps.newHashMap();
  private transient OperatorContext context;

  /**
   * Size limit for data files. Files are rolled once the limit has been exceeded. The final size of a file can be
   * larger than the limit by the size of the last/single entry written to it.
   *
   * @return The size limit for data files.
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
   * Size limit for WAL files. Files are rolled once the limit has been exceeded. The final size of a file can be larger
   * than the limit, as files are rolled at end of the operator window.
   *
   * @return The size limit for WAL files.
   */
  public int getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(int maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
  }

  /**
   * The number of changes collected in memory before flushing to persistent storage.
   *
   * @return The number of changes collected in memory before flushing to persistent storage.
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
   * Cached writes are flushed to persistent storage periodically. The interval is specified as count of windows and
   * establishes the maximum latency for changes to be written while below the {@link #flushSize} threshold.
   *
   * @return The flush interval count.
   */
  @Min(value = 1)
  public int getFlushIntervalCount()
  {
    return flushIntervalCount;
  }

  public void setFlushIntervalCount(int flushIntervalCount)
  {
    this.flushIntervalCount = flushIntervalCount;
  }

  /**
   * Write data to size based rolling files
   *
   * @param bucket
   * @param bucketMeta
   * @param data
   * @throws IOException
   */
  private void writeFile(Bucket bucket, BucketMeta bucketMeta, TreeMap<Slice, byte[]> data) throws IOException
  {
    BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
    long startTime = System.currentTimeMillis();

    HDSFileWriter fw = null;
    BucketFileMeta fileMeta = null;
    int keysWritten = 0;
    for (Map.Entry<Slice, byte[]> dataEntry : data.entrySet()) {
      if (fw == null) {
        // next file
        fileMeta = bucketMeta.addFile(bucket.bucketKey, dataEntry.getKey());
        LOG.debug("writing new data file {} {}", bucket.bucketKey, fileMeta.name);
        fw = this.store.getWriter(bucket.bucketKey, fileMeta.name + ".tmp");
        keysWritten = 0;
      }

      fw.append(HDS.SliceExt.asArray(dataEntry.getKey()), dataEntry.getValue());
      keysWritten++;
      if (fw.getBytesWritten() > this.maxFileSize) {
        ioStats.dataFilesWritten++;
        ioStats.filesWroteInCurrentWriteCycle++;
        // roll file
        fw.close();
        ioStats.dataBytesWritten += fw.getBytesWritten();
        this.store.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        LOG.debug("created data file {} {} with {} entries", bucket.bucketKey, fileMeta.name, keysWritten);
        fw = null;
        keysWritten = 0;
      }
    }

    if (fw != null) {
      ioStats.dataFilesWritten++;
      ioStats.filesWroteInCurrentWriteCycle++;
      fw.close();
      ioStats.dataBytesWritten += fw.getBytesWritten();
      this.store.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
      LOG.debug("created data file {} {} with {} entries", bucket.bucketKey, fileMeta.name, keysWritten);
    }

    ioStats.dataWriteTime += System.currentTimeMillis() - startTime;
  }

  private Bucket getBucket(long bucketKey) throws IOException
  {
    Bucket bucket = this.buckets.get(bucketKey);
    if (bucket == null) {
      LOG.debug("Opening bucket {}", bucketKey);
      bucket = new Bucket();
      bucket.bucketKey = bucketKey;
      this.buckets.put(bucketKey, bucket);

      BucketMeta bmeta = getMeta(bucketKey);
      WalMeta wmeta = getWalMeta(bucketKey);
      bucket.wal = new HDSWalManager(this.store, bucketKey, wmeta.fileId, wmeta.offset);
      bucket.wal.setMaxWalFileSize(maxWalFileSize);
      BucketIOStats ioStats = getOrCretaStats(bucketKey);
      if (ioStats != null)
        bucket.wal.restoreStats(ioStats);
      LOG.debug("Existing walmeta information fileId {} offset {} tailId {} tailOffset {} windowId {} committedWid {} currentWid {}",
          wmeta.fileId, wmeta.offset, wmeta.tailId, wmeta.tailOffset, wmeta.windowId, bmeta.committedWid, windowId);

      // bmeta.componentLSN is data which is committed to disks.
      // wmeta.windowId windowId till which data is available in WAL.
      if (bmeta.committedWid < wmeta.windowId && wmeta.windowId != 0) {
        LOG.debug("Recovery for bucket {}", bucketKey);
        // Get last committed LSN from store, and use that for recovery.
        bucket.wal.runRecovery(bucket.writeCache, wmeta.tailId, wmeta.tailOffset);
      }
    }
    return bucket;
  }

  /**
   * Lookup in write cache (data not flushed/committed to files).
   * @param bucketKey
   * @param key
   * @return uncommitted.
   */
  @Override
  public byte[] getUncommitted(long bucketKey, Slice key)
  {
    Bucket bucket = this.buckets.get(bucketKey);
    if (bucket != null) {
      byte[] v = bucket.writeCache.get(key);
      if (v != null) {
        return v;
      }
      return bucket.frozenWriteCache.get(key);
    }
    return null;
  }

  /**
   * Intercept query processing to incorporate unwritten changes.
   */
  @Override
  protected void processQuery(HDSQuery query)
  {
    // check unwritten changes first
    byte[] v = getUncommitted(query.bucketKey, query.key);
    if (v != null) {
      query.result = v;
      query.processed = true;
      return;
    }
    super.processQuery(query);
  }

  @Override
  public void put(long bucketKey, Slice key, byte[] value) throws IOException
  {
    Bucket bucket = getBucket(bucketKey);
    bucket.wal.append(key, value);
    bucket.writeCache.put(key, value);
  }

  /**
   * Flush changes from write cache to disk. New data files will be written and meta data replaced atomically. The flush
   * frequency determines availability of changes to external readers.
   *
   * @throws IOException
   */
  private void writeDataFiles(Bucket bucket) throws IOException
  {
    BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
    LOG.debug("Writing data files in bucket {}", bucket.bucketKey);
    // copy meta data on write
    BucketMeta bucketMetaCopy = kryo.copy(getMeta(bucket.bucketKey));

    // bucket keys by file
    TreeMap<Slice, BucketFileMeta> bucketSeqStarts = bucketMetaCopy.files;
    Map<BucketFileMeta, Map<Slice, byte[]>> modifiedFiles = Maps.newHashMap();

    for (Map.Entry<Slice, byte[]> entry : bucket.frozenWriteCache.entrySet()) {
      // find file for key
      Map.Entry<Slice, BucketFileMeta> floorEntry = bucketSeqStarts.floorEntry(entry.getKey());
      BucketFileMeta floorFile;
      if (floorEntry != null) {
        floorFile = floorEntry.getValue();
      } else {
        floorEntry = bucketSeqStarts.firstEntry();
        if (floorEntry == null || floorEntry.getValue().name != null) {
          // no existing file or file with higher key
          floorFile = new BucketFileMeta();
        } else {
          // placeholder for new keys, move start key
          floorFile = floorEntry.getValue();
          bucketSeqStarts.remove(floorEntry.getKey());
        }
        floorFile.startKey = entry.getKey();
        if (floorFile.startKey.length != floorFile.startKey.buffer.length) {
          // normalize key for serialization
          floorFile.startKey = floorFile.startKey.clone();
        }
        bucketSeqStarts.put(floorFile.startKey, floorFile);
      }

      Map<Slice, byte[]> fileUpdates = modifiedFiles.get(floorFile);
      if (fileUpdates == null) {
        modifiedFiles.put(floorFile, fileUpdates = Maps.newHashMap());
      }
      fileUpdates.put(entry.getKey(), entry.getValue());
    }

    HashSet<String> filesToDelete = Sets.newHashSet();

    // write modified files
    for (Map.Entry<BucketFileMeta, Map<Slice, byte[]>> fileEntry : modifiedFiles.entrySet()) {
      BucketFileMeta fileMeta = fileEntry.getKey();
      TreeMap<Slice, byte[]> fileData = new TreeMap<Slice, byte[]>(getKeyComparator());

      if (fileMeta.name != null) {
        // load existing file
        long start = System.currentTimeMillis();
        HDSFileReader reader = store.getReader(bucket.bucketKey, fileMeta.name);
        reader.readFully(fileData);
        ioStats.dataBytesRead += store.getFileSize(bucket.bucketKey, fileMeta.name);
        ioStats.dataReadTime += System.currentTimeMillis() - start;
        /* these keys are re-written */
        ioStats.dataKeysRewritten += fileData.size();
        ioStats.filesReadInCurrentWriteCycle++;
        ioStats.dataFilesRead++;
        reader.close();
        filesToDelete.add(fileMeta.name);
      }

      // apply updates
      fileData.putAll(fileEntry.getValue());
      // new file
      writeFile(bucket, bucketMetaCopy, fileData);
    }

    LOG.debug("Number of file read {} Number of files wrote {} in this write cycle",
        ioStats.filesWroteInCurrentWriteCycle, ioStats.filesReadInCurrentWriteCycle);
    // flush meta data for new files
    try {
      LOG.debug("writing {} with {} file entries", FNAME_META, bucketMetaCopy.files.size());
      OutputStream os = store.getOutputStream(bucket.bucketKey, FNAME_META + ".new");
      Output output = new Output(os);
      bucketMetaCopy.committedWid = windowId;
      kryo.writeClassAndObject(output, bucketMetaCopy);
      output.close();
      os.close();
      store.rename(bucket.bucketKey, FNAME_META + ".new", FNAME_META);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write bucket meta data " + bucket.bucketKey, e);
    }

    // clear pending changes
    ioStats.dataKeysWritten += bucket.frozenWriteCache.size();
    bucket.frozenWriteCache.clear();
    // switch to new version
    this.metaCache.put(bucket.bucketKey, bucketMetaCopy);

    // delete old files
    for (String fileName : filesToDelete) {
      store.delete(bucket.bucketKey, fileName);
    }
    invalidateReader(bucket.bucketKey, filesToDelete);

    WalMeta walMeta = getWalMeta(bucket.bucketKey);
    walMeta.tailId = bucket.tailId;
    walMeta.tailOffset = bucket.tailOffset;

    bucket.wal.cleanup(walMeta.tailId);
    ioStats.filesReadInCurrentWriteCycle = 0;
    ioStats.filesWroteInCurrentWriteCycle = 0;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    writeExecutor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(this.getClass().getSimpleName() + "-Writer"));
    this.context = context;
  }

  @Override
  public void teardown()
  {
    for (Bucket bucket : this.buckets.values()) {
      IOUtils.closeQuietly(bucket.wal);
    }
    writeExecutor.shutdown();
    super.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    for (final Bucket bucket : this.buckets.values()) {
      try {
        if (bucket.wal != null) {
          bucket.wal.endWindow(windowId);
          WalMeta walMeta = getWalMeta(bucket.bucketKey);
          walMeta.fileId = bucket.wal.getWalFileId();
          walMeta.offset = bucket.wal.getCommittedLength();
          walMeta.windowId = windowId;
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to flush WAL", e);
      }

      if ((bucket.writeCache.size() > this.flushSize || windowId - lastFlushWindowId > flushIntervalCount) && !bucket.writeCache.isEmpty()) {
        // ensure previous flush completed
        if (bucket.frozenWriteCache.isEmpty()) {
          bucket.frozenWriteCache = bucket.writeCache;

          bucket.committedLSN = windowId;
          bucket.tailId = bucket.wal.getWalFileId();
          bucket.tailOffset = bucket.wal.getCommittedLength();

          bucket.writeCache = Maps.newHashMap();
          LOG.debug("Flushing data for bucket {} committedWid {}", bucket.bucketKey, bucket.committedLSN);
          Runnable flushRunnable = new Runnable() {
            @Override
            public void run()
            {
              try {
                writeDataFiles(bucket);
              } catch (Throwable e) {
                LOG.debug("Write error: {}", e.getMessage());
                writerError = e;
              }
            }
          };
          this.writeExecutor.execute(flushRunnable);
          lastFlushWindowId = windowId;
        }
      }
    }

    // propagate writer exceptions
    if (writerError != null) {
      throw new RuntimeException("Error while flushing write cache.", this.writerError);
    }

    if (context != null) {
      updateStats();
      context.setCounters(bucketStats);
    }
  }

  private WalMeta getWalMeta(long bucketKey)
  {
    WalMeta meta = walMeta.get(bucketKey);
    if (meta == null) {
      meta = new WalMeta();
      walMeta.put(bucketKey, meta);
    }
    return meta;
  }

  @Override
  public void checkpointed(long arg0)
  {
  }

  /**
   * Get meta data from cache or load it on first access
   *
   * @param bucketKey
   * @return The bucket meta.
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

  @Override
  public void committed(long committedWindowId)
  {
  }

  private static class Bucket
  {
    private long bucketKey;
    // keys that were modified and written to WAL, but not yet persisted
    private HashMap<Slice, byte[]> writeCache = Maps.newHashMap();
    // keys that are being flushed to data files
    private HashMap<Slice, byte[]> frozenWriteCache = Maps.newHashMap();
    private HDSWalManager wal;
    private long committedLSN;
    private long tailId;
    private long tailOffset;
  }

  @VisibleForTesting
  protected void forceWal() throws IOException
  {
    for (Bucket bucket : buckets.values()) {
      bucket.wal.close();
    }
  }

  @VisibleForTesting
  protected int unflushedData(long bucketKey) throws IOException
  {
    Bucket b = getBucket(bucketKey);
    return b.writeCache.size();
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDSWriter.class);

  /* Holds current file Id for WAL and current offset for WAL */
  private static class WalMeta
  {
    /* The current WAL file and offset */
    // Window Id which is written to the WAL.
    public long windowId;
    // Current Wal File sequence id
    long fileId;
    // Offset in current file after writing data for windowId.
    long offset;

    /* Flushed WAL file and offset, data till this point is flushed to disk */
    public long tailId;
    public long tailOffset;
  }

  @JsonSerialize
  public static class BucketIOStats implements Serializable
  {
    /* Bytes written to the WAL till now */
    public long walBytesWritten;
    /* Number of times WAL was flushed */
    public long walFlushCount;
    /* Amount of time spent while waiting for WAL flush to disk in milliseconds */
    public long walFlushTime;
    /* wal keys written */
    public long walKeysWritten;


    /* Number of data files written */
    public long dataFilesWritten;
    /* Number of bytes written to data files */
    public long dataBytesWritten;
    /* Time taken for writing files */
    public long dataWriteTime;
    /* total keys written to data files */
    public long dataKeysWritten;
    /* The number of keys which are re-written, i.e keys which are read into memory from existing
       files which are written again in new data files */
    public long dataKeysRewritten;

    /* records in memory */
    public long dataInWriteCache;
    public long dataInFrozenCache;
    public int filesReadInCurrentWriteCycle;
    public int filesWroteInCurrentWriteCycle;

    public int dataFilesRead;
    /* Total time spent in reading files during write */
    public long dataReadTime;
    /* Number of bytes read during data read */
    public long dataBytesRead;

    @Override public String toString()
    {
      return "BucketIOStats{" +
          "walBytesWritten=" + walBytesWritten +
          ", walFlushCount=" + walFlushCount +
          ", walFlushTime=" + walFlushTime +
          ", walKeysWritten=" + walKeysWritten +
          ", dataFilesWritten=" + dataFilesWritten +
          ", dataBytesWritten=" + dataBytesWritten +
          ", dataWriteTime=" + dataWriteTime +
          ", dataKeysWritten=" + dataKeysWritten +
          ", dataKeysRewritten=" + dataKeysRewritten +
          ", dataInWriteCache=" + dataInWriteCache +
          ", dataInFrozenCache=" + dataInFrozenCache +
          ", filesReadInCurrentWriteCycle=" + filesReadInCurrentWriteCycle +
          ", filesWroteInCurrentWriteCycle=" + filesWroteInCurrentWriteCycle +
          ", dataFilesRead=" + dataFilesRead +
          ", dataReadTime=" + dataReadTime +
          ", dataBytesRead=" + dataBytesRead +
          '}';
    }
  }

  private void updateStats()
  {
    for(Bucket bucket : buckets.values())
    {
      BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
      /* fill in stats for WAL */
      HDSWalManager.WalStats walStats = bucket.wal.getCounters();
      ioStats.walBytesWritten = walStats.totalBytes;
      ioStats.walFlushCount = walStats.flushCounts;
      ioStats.walFlushTime = walStats.flushDuration;
      ioStats.walKeysWritten = walStats.totalKeys;
      ioStats.dataInWriteCache = bucket.writeCache.size();
      ioStats.dataInFrozenCache = bucket.frozenWriteCache.size();
    }
  }

  @JsonSerialize
  public static class AggregatedBucketIOStats implements Serializable {
    public BucketIOStats globalStats = new BucketIOStats();
    /* Individual bucket stats */
    public Map<Long, BucketIOStats> aggregatedStats = Maps.newHashMap();
  }

  public static class BucketIOStatAggregator implements Serializable, Context.CountersAggregator
  {
    @Override public Object aggregate(Collection<?> countersList)
    {
      AggregatedBucketIOStats aggStats = new AggregatedBucketIOStats();
      for(Object o : countersList)
      {
        Map<Long, BucketIOStats> statMap = (Map<Long, BucketIOStats>)o;
        for(Long bId : statMap.keySet())
        {
          BucketIOStats stats = statMap.get(bId);
          aggStats.globalStats.walBytesWritten += stats.walBytesWritten;
          aggStats.globalStats.walFlushCount += stats.walFlushCount;
          aggStats.globalStats.walFlushTime += stats.walFlushTime;
          aggStats.globalStats.walKeysWritten += stats.walKeysWritten;

          aggStats.globalStats.dataWriteTime += stats.dataWriteTime;
          aggStats.globalStats.dataFilesWritten += stats.dataFilesWritten;
          aggStats.globalStats.dataBytesWritten += stats.dataBytesWritten;
          aggStats.globalStats.dataKeysWritten += stats.dataKeysWritten;
          aggStats.globalStats.dataKeysRewritten += stats.dataKeysRewritten;

          aggStats.globalStats.dataInWriteCache += stats.dataInWriteCache;
          aggStats.globalStats.dataInFrozenCache += stats.dataInFrozenCache;
          aggStats.globalStats.filesReadInCurrentWriteCycle += stats.filesReadInCurrentWriteCycle;
          aggStats.globalStats.filesWroteInCurrentWriteCycle += stats.filesWroteInCurrentWriteCycle;

          aggStats.globalStats.dataReadTime += stats.dataReadTime;
          aggStats.globalStats.dataFilesRead += stats.dataFilesRead;
          aggStats.globalStats.dataBytesRead += stats.dataBytesRead;

          aggStats.aggregatedStats.put(bId, stats);
        }
      }
      return aggStats;
    }
  }

  /* A map holding stats for each bucket written by this partition */
  private HashMap<Long, BucketIOStats> bucketStats = Maps.newHashMap();

  private BucketIOStats getOrCretaStats(long bucketKey)
  {
    BucketIOStats ioStats = bucketStats.get(bucketKey);
    if (ioStats == null) {
      ioStats = new BucketIOStats();
      bucketStats.put(bucketKey, ioStats);
    }
    return ioStats;
  }
}

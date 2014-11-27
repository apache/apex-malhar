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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.HDS.WALReader;
import com.datatorrent.contrib.hds.HDS.WALWriter;
import com.google.common.collect.Maps;

/**
 * Manages WAL for a bucket.
 * When a tuple is added to WAL, is it immediately written to the
 * file, but not flushed, flushing happens at end of the operator
 * window during endWindow call. At end of window if WAL file size
 * have grown a beyond maxWalFileSize then current file is closed
 * and new file is created.
 *
 * The WAL usage windowId as log sequence number(LSN). When data is
 * written to data files, the committedWid saved in bucket metadata.
 *
 * The windowId upto which data is available is stored in BucketManager
 * WalMetadata and check pointed with operator state.
 *
 * Recovery After Failure.
 *
 *   If committedWid is smaller than wal windowId.
 *   - Truncate last WAL file to known offset.
 *   - Wal metadata contains file id and offset where committedWid ended,
 *     start reading from that location till the end of current WAL file
 *     and adds tuples back to the writeCache in store.
 *
 *   If committedWid is greater than wal windowId
 *   The data was committed to disks after last operator checkpoint. In this
 *   case recovery is not needed all data from WAL is already written to data
 *   files. We will reprocess tuples which are in between committedWid and wal windowId.
 *   This will not cause problem now, because file write is idempotent with
 *   duplicate tuples.
 *
 */
public class HDSWalManager implements Closeable
{
  public static final String WAL_FILE_PREFIX = "_WAL-";

  public void setBucketKey(long bucketKey)
  {
    this.bucketKey = bucketKey;
  }

  public void restoreStats(HDSWriter.BucketIOStats ioStats)
  {
    if (stats != null) {
      stats.flushCounts = ioStats.walFlushCount;
      stats.flushDuration = ioStats.walFlushTime;
      stats.totalBytes = ioStats.walBytesWritten;
      stats.totalKeys = ioStats.walKeysWritten;
    }
  }

  private static class WindowEntry {
    long walFileId;
    long offset;

    public WindowEntry(long walFileId, long committedLength)
    {
      this.walFileId = walFileId;
      this.offset = committedLength;
    }
  }

  public TreeMap<Long, WindowEntry> windowIndex = Maps.newTreeMap();

  /* Backing file system for WAL */
  transient HDSFileAccess bfs;

  /*
   * If maximum number of bytes allowed to be written to file between flush,
   * default is 64K.
   */
  transient long maxUnflushedBytes = 64 * 1024;

  /* Maximum number of bytes per WAL file,
   * default is 128M */
  transient long maxWalFileSize = 128 * 1024 * 1024;

  /* The class responsible writing WAL entry to file */
  transient WALWriter writer;

  transient private long bucketKey;

  private boolean dirty;

  /* current active WAL file id, it is read from WAL meta on startup */
  private long walFileId = -1;

  /* Last committed LSN on disk */
  private long committedLsn = -1;

  /* Current WAL size */
  private long committedLength = 0;

  @SuppressWarnings("unused")
  private HDSWalManager() {}

  public HDSWalManager(HDSFileAccess bfs, long bucketKey) {
    this.bfs = bfs;
    this.bucketKey = bucketKey;
  }

  public HDSWalManager(HDSFileAccess bfs, long bucketKey, long fileId, long offset) {
    this.bfs = bfs;
    this.bucketKey = bucketKey;
    this.walFileId = fileId;
    this.committedLength = offset;
    logger.info("current {}  offset {} ", walFileId, committedLength);
  }

  /**
   * Run recovery for bucket, by adding valid data from WAL to
   * store.
   */
  public void runRecovery(Map<Slice, byte[]> writeCache, long fileId, long offset) throws IOException
  {
    if (walFileId == 0 && committedLength == 0)
      return;

    restoreLastWal();

    /* Reconstruct Store cache state, from WAL files */
    long walid = fileId;
    logger.info("Recovery of store, start file {} offset {} till file {} offset {}",
        walid, offset, walFileId, committedLength);
    for (long i = fileId; i <= walFileId; i++) {
      WALReader wReader = new HDFSWalReader(bfs, bucketKey, WAL_FILE_PREFIX + i);
      wReader.seek(offset);
      offset = 0;
      int count = 0;
      while (wReader.advance()) {
        MutableKeyValue o = wReader.get();
        writeCache.put(HDS.SliceExt.toSlice(o.getKey()), o.getValue());
        count++;
      }
      wReader.close();
      logger.info("Recovered {} tuples from wal {}", count, i);
    }

    walFileId++;
  }

  /**
   *  Restore state of wal just after last checkpoint. The DT platform
   *  will resend tuple after last operator checkpoint to the WAL, this will result
   *  in duplicate tuples in WAL, if we don't restore the WAL just after
   *  checkpoint state.
   */
  private void restoreLastWal() throws IOException
  {
    if (committedLength == 0)
      return;
    logger.info("recover wal file {}, data valid till offset {}", walFileId, committedLength);
    DataInputStream in = bfs.getInputStream(bucketKey, WAL_FILE_PREFIX + walFileId);
    DataOutputStream out = bfs.getOutputStream(bucketKey, WAL_FILE_PREFIX + walFileId + "-truncate");
    IOUtils.copyLarge(in, out, 0, committedLength);
    in.close();
    out.close();
    bfs.rename(bucketKey, WAL_FILE_PREFIX + walFileId + "-truncate", WAL_FILE_PREFIX + walFileId);
  }

  public void append(Slice key, byte[] value) throws IOException
  {
    if (writer == null)
      writer = new HDFSWalWriter(bfs, bucketKey, WAL_FILE_PREFIX + walFileId);

    writer.append(key, value);
    long bytes = key.length + value.length + 2 * 4;
    stats.totalBytes += bytes;
    stats.totalKeys ++;
    dirty = true;

    if (maxUnflushedBytes > 0 && writer.getUnflushedCount() > maxUnflushedBytes)
    {
      flushWal();
    }
  }

  protected void flushWal() throws IOException
  {
    if (writer == null)
      return;
    long startTime = System.currentTimeMillis();
    writer.flush();

    stats.flushCounts++;
    stats.flushDuration += System.currentTimeMillis() - startTime;
  }

  /* Update WAL meta data after committing window id wid */
  public void updateWalMeta(long wid) {
    windowIndex.put(wid, new WindowEntry(walFileId, committedLength));
  }

  /* batch writes, and wait till file is written */
  public void endWindow(long windowId) throws IOException
  {
    /* No tuple added in this window, no need to do anything. */
    if (!dirty)
      return;

    flushWal();

    dirty = false;
    committedLsn = windowId;
    committedLength = writer.logSize();
    updateWalMeta(windowId);

    /* Roll over log, if we have crossed the log size */
    if (maxWalFileSize > 0 && writer.logSize() > maxWalFileSize) {
      logger.info("Rolling over log {} windowid {}", writer, windowId);
      writer.close();
      walFileId++;
      writer = null;
      committedLength = 0;
    }
  }

  /**
   * Remove files older than tailId.
   * @param tailId
   */
  public void cleanup(long tailId)
  {
    if (tailId == 0)
      return;

    tailId--;
    try {
      while (true) {
        DataInputStream in = bfs.getInputStream(bucketKey, WAL_FILE_PREFIX + tailId);
        in.close();
        logger.info("deleting WAL file {}", tailId);
        bfs.delete(bucketKey, WAL_FILE_PREFIX + tailId);
        tailId--;
      }
    } catch (FileNotFoundException ex) {
    } catch (IOException ex) {
    }
  }

  public long getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(long maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
  }

  public long getMaxUnflushedBytes()
  {
    return maxUnflushedBytes;
  }

  public void setMaxUnflushedBytes(long maxUnflushedBytes)
  {
    this.maxUnflushedBytes = maxUnflushedBytes;
  }

  public long getCommittedLSN() {
    return committedLsn;
  }

  @Override
  public void close() throws IOException
  {
    if (writer != null)
      writer.close();
  }

  public long getWalFileId()
  {
    return walFileId;
  }

  public long getCommittedLength()
  {
    return committedLength;
  }

  public void setFileStore(HDSFileAccess bfs)
  {
    this.bfs = bfs;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(HDSWalManager.class);

  /**
   * Stats related functionality
   */
  public static class WalStats
  {
    long totalBytes;
    long flushCounts;
    long flushDuration;
    public long totalKeys;
  }

  private WalStats stats = new WalStats();

  public WalStats getCounters() {
    return stats;
  }
}

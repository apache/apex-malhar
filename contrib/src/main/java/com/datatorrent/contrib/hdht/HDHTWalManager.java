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
package com.datatorrent.contrib.hdht;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.Slice;
import com.datatorrent.contrib.hdht.HDHT.WALReader;
import com.datatorrent.contrib.hdht.HDHT.WALWriter;

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
 * WalMetadata and checkpointed with operator state.
 *
 * Recovery After Failure.
 *
 *   If committedWid is smaller than wal windowId.
 *   - Truncate last WAL file to known offset (recoveryEndWalOffset).
 *   - Wal metadata contains file id and recoveryEndWalOffset where committedWid ended,
 *     start reading from that location till the end of current WAL file
 *     and adds tuples back to the committed cache in store.
 *
 *   If committedWid is greater than wal windowId
 *   The data was committed to disks after last operator checkpoint. In this
 *   case recovery is not needed all data from WAL is already written to data
 *   files. We will reprocess tuples which are in between committedWid and wal windowId.
 *   This will not cause problem now, because file write is idempotent with
 *   duplicate tuples.
 *
 * @since 2.0.0 
 */
public class HDHTWalManager implements Closeable
{
  public static final String WAL_FILE_PREFIX = "_WAL-";

  public void setBucketKey(long bucketKey)
  {
    this.bucketKey = bucketKey;
  }

  public void restoreStats(HDHTWriter.BucketIOStats ioStats)
  {
    if (stats != null) {
      stats.flushCounts = ioStats.walFlushCount;
      stats.flushDuration = ioStats.walFlushTime;
      stats.totalBytes = ioStats.walBytesWritten;
      stats.totalKeys = ioStats.walKeysWritten;
    }
  }

  /* Backing file system for WAL */
  transient HDHTFileAccess bfs;

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

  /* Last committed LSN on disk */
  private long flushedWid = -1;

  /* current active WAL file id, it is read from WAL meta on startup */
  private long walFileId = -1;

  /* Current WAL size */
  private long walSize = 0;

  @SuppressWarnings("unused")
  private HDHTWalManager() {}

  public HDHTWalManager(HDHTFileAccess bfs, long bucketKey) {
    this.bfs = bfs;
    this.bucketKey = bucketKey;
  }

  public HDHTWalManager(HDHTFileAccess bfs, long bucketKey, WalPosition walPos) {
    this(bfs, bucketKey);
    this.walFileId = walPos == null? 0 : walPos.fileId;
    this.walSize = walPos == null? 0 : walPos.offset;
    logger.info("current {}  offset {} ", walFileId, walSize);
  }

  public HDHTWalManager(HDHTFileAccess bfs, long bucketKey, long fileId, long offset) {
    this.bfs = bfs;
    this.bucketKey = bucketKey;
    this.walFileId = fileId;
    this.walSize = offset;
    logger.info("current {}  offset {} ", walFileId, walSize);
  }

  /**
   * Run recovery for bucket, by adding valid data from WAL to
   * store.
   */
  public void runRecovery(Map<Slice, byte[]> writeCache, WalPosition start, WalPosition end) throws IOException {
    if (end.fileId == 0 && end.offset == 0)
      return;

    /* Make sure that WAL state is correctly restored */
    truncateWal(end);

    logger.info("Recovery of store, start {} till {}",
        start, end);

    long offset = start.offset;
    for (long i = start.fileId; i <= end.fileId; i++) {
      WALReader wReader = new HDFSWalReader(bfs, bucketKey, WAL_FILE_PREFIX + i);
      wReader.seek(offset);
      offset = 0;
      int count = 0;
      while (wReader.advance()) {
        MutableKeyValue o = wReader.get();
        writeCache.put(new Slice(o.getKey()), o.getValue());
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
  private void truncateWal(WalPosition pos) throws IOException
  {
    if (pos.offset == 0)
      return;
    logger.info("recover wal file {}, data valid till offset {}", pos.fileId, pos.offset);
    DataInputStream in = bfs.getInputStream(bucketKey, WAL_FILE_PREFIX + pos.fileId);
    DataOutputStream out = bfs.getOutputStream(bucketKey, WAL_FILE_PREFIX + pos.fileId + "-truncate");
    IOUtils.copyLarge(in, out, 0, pos.offset);
    in.close();
    out.close();
    bfs.rename(bucketKey, WAL_FILE_PREFIX + pos.fileId + "-truncate", WAL_FILE_PREFIX + pos.fileId);
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

  /* batch writes, and wait till file is written */
  public void endWindow(long windowId) throws IOException
  {
    /* No tuple added in this window, no need to do anything. */
    if (!dirty)
      return;

    flushWal();

    dirty = false;
    flushedWid = windowId;
    walSize = writer.logSize();

    /* Roll over log, if we have crossed the log size */
    if (maxWalFileSize > 0 && writer.logSize() > maxWalFileSize) {
      logger.info("Rolling over log {} windowid {}", writer, windowId);
      writer.close();
      walFileId++;
      writer = null;
      walSize = 0;
    }
  }

  /**
   * Remove files older than recoveryStartWalFileId.
   * @param recoveryStartWalFileId
   */
  public void cleanup(long recoveryStartWalFileId)
  {
    if (recoveryStartWalFileId == 0)
      return;

    recoveryStartWalFileId--;
    try {
      while (true) {
        DataInputStream in = bfs.getInputStream(bucketKey, WAL_FILE_PREFIX + recoveryStartWalFileId);
        in.close();
        logger.info("deleting WAL file {}", recoveryStartWalFileId);
        bfs.delete(bucketKey, WAL_FILE_PREFIX + recoveryStartWalFileId);
        recoveryStartWalFileId--;
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

  public long getFlushedWid() {
    return flushedWid;
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

  public long getWalSize()
  {
    return walSize;
  }

  public void setFileStore(HDHTFileAccess bfs)
  {
    this.bfs = bfs;
  }

  public WalPosition getCurrentPosition() {
    return new WalPosition(walFileId, walSize);
  }

  private static transient final Logger logger = LoggerFactory.getLogger(HDHTWalManager.class);

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

  private final WalStats stats = new WalStats();

  /* Location of the WAL */
  public static class WalPosition {
    long fileId;
    long offset;

    private WalPosition() {
    }

    public WalPosition(long fileId, long offset) {
      this.fileId = fileId;
      this.offset = offset;
    }

    public WalPosition copyOf() {
      return new WalPosition(fileId, offset);
    }

    @Override public String toString()
    {
      return "WalPosition{" +
          "fileId=" + fileId +
          ", offset=" + offset +
          '}';
    }
  }

  public WalStats getCounters() {
    return stats;
  }
}

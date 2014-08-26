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

import com.datatorrent.contrib.hds.HDS.WALReader;
import com.datatorrent.contrib.hds.HDS.WALWriter;
import com.google.common.collect.Maps;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

/**
 * Manages WAL for a bucket.
 * When a tuple is added to WAL, is it immediately written to the
 * file, but not flushed, flushing happens at end of the operator
 * window during endWindow call. At end of window if WAL file size
 * have grown a beyond maxWalFileSize then current file is closed
 * and new file is created.
 *
 * The WAL manages LSN for each bucket separately, LSN start from
 * zero and incremented at each operator window. LSN is saved as
 * part of WAL state.
 *
 * WAL state contains following files.
 * 1) Current WAL file id (A integer , which is added to WAL file name
 *   for example _WAL-id
 * 2) committedLSN  data for this and all previous LSNs are flushed to disks.
 * 3) For each file
 *     Start LSN and End LSN.
 *     for each LSN start and end offset.
 *
 * After failure
 *   next WAL file id used for storing logs.
 *   active LSN is set to committedLSN + 1
 *   find file and offset for persistedLSN in data files and read
 *   all files after that to recovery store state.
 *
 */
public class BucketWalWriter implements Closeable
{
  public static final String WAL_FILE_PREFIX = "_WAL-";

  public void setBucketKey(long bucketKey)
  {
    this.bucketKey = bucketKey;
  }

  static class OffsetRange {
    long start;
    long end;

    @SuppressWarnings("unused")
    private OffsetRange() {}

    OffsetRange(long start, long end) {
      this.start = start;
      this.end = end;
    }
  }

  /* Data to be maintain for each file */
  private static class WalFileMeta {
    public long walId;
    public long walSequenceStart;
    public long walSequenceEnd;
    public long committedBytes;
    public TreeMap<Long, OffsetRange> windowIndex = Maps.newTreeMap();


    @Override public String toString()
    {
      return "WalFileMeta{" +
          "walId=" + walId +
          ", walSequenceStart=" + walSequenceStart +
          ", walSequenceEnd=" + walSequenceEnd +
          ", committedBytes=" + committedBytes +
          '}';
    }
  }

  /* Backing file system for WAL */
  transient HDSFileAccess bfs;

  /*
   * If maximum number of bytes allowed to be written to file between flush,
   * default is 64K.
   */
  transient long maxUnflushedBytes = 64 * 1024;

  /* Maximum number of bytes per WAL file,
   * default is 100M */
  transient long maxWalFileSize = 512 * 1024;

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

  public TreeMap<Long, WalFileMeta> files = Maps.newTreeMap();

  @SuppressWarnings("unused")
  private BucketWalWriter() {}

  public BucketWalWriter(HDSFileAccess bfs, long bucketKey) {
    this.bfs = bfs;
    this.bucketKey = bucketKey;
  }

  public BucketWalWriter(HDSFileAccess bfs, long bucketKey, long fileId, long offset) {
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
  public void runRecovery(HDS.BucketManager store, long fileId, long offset) throws IOException
  {
    if (walFileId == 0 && committedLength == 0)
      return;

    restoreLastWal();

    /* Reconstruct Store cache state, from WAL files */
    long walid = fileId;
    logger.info("Recovering starting from file " + walid + " offset " + offset + " till " + walFileId);
    for (long i = fileId; i <= walFileId; i++) {
      WALReader wReader = new HDFSWalReader(bfs, bucketKey, WAL_FILE_PREFIX + i);
      wReader.seek(offset);
      offset = 0;

      while (wReader.advance()) {
        logger.info("Reading tuple");
        MutableKeyValue o = wReader.get();
        store.put(bucketKey, o.getKey(), o.getValue());
      }
      wReader.close();
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
    logger.info("recovery wal {} till offset {}", walFileId, committedLength);
    DataInputStream in = bfs.getInputStream(bucketKey, WAL_FILE_PREFIX + walFileId);
    DataOutputStream out = bfs.getOutputStream(bucketKey, WAL_FILE_PREFIX + walFileId + "-truncate");
    IOUtils.copyLarge(in, out, 0, committedLength);
    in.close();
    out.close();
    bfs.rename(bucketKey, WAL_FILE_PREFIX + walFileId + "-truncate", WAL_FILE_PREFIX + walFileId);
    logger.info("Done copying file");
    //TODO: Remove new WAL files created after last checkpoint (i.e fileId > walFileId).
  }

  public void append(byte[] key, byte[] value) throws IOException
  {
    if (writer == null)
      writer = new HDFSWalWriter(bfs, bucketKey, WAL_FILE_PREFIX + walFileId);

    writer.append(key, value);
    dirty = true;

    if (maxUnflushedBytes > 0 && writer.getUnflushedCount() > maxUnflushedBytes)
      writer.flush();
  }

  /* Update WAL meta data after committing window id wid */
  public void updateWalMeta() {
    WalFileMeta fileMeta = files.get(walFileId);
    if (fileMeta == null) {
      fileMeta = new WalFileMeta();
      fileMeta.walId = walFileId;
      fileMeta.walSequenceStart = committedLsn;
      files.put(walFileId, fileMeta);
    }

    logger.debug("file " + walFileId + " lsn " + committedLsn + " start " + committedLength + " end " + writer.logSize());
    OffsetRange range = new OffsetRange(committedLength, writer.logSize());
    fileMeta.windowIndex.put(committedLsn, range);
    fileMeta.walSequenceEnd = committedLsn;
    committedLength = writer.logSize();
    fileMeta.committedBytes = committedLength;
  }

  /* batch writes, and wait till file is written */
  public void endWindow(long windowId) throws IOException
  {
    /* No tuple added in this window, no need to do anything. */
    if (!dirty)
      return;

    if (writer != null) {
      writer.flush();
    }
    dirty = false;
    committedLsn = windowId;

    updateWalMeta();

    /* Roll over log, if we have crossed the log size */
    if (maxWalFileSize > 0 && writer.logSize() > maxWalFileSize) {
      logger.info("Rolling over log {}", writer);
      writer.close();
      walFileId++;
      writer = null;
      committedLength = 0;
    }
  }



  /**
   * Remote all WAL file, and cleanup metadata.
   * @throws IOException
   */
  private void cleanupAllWal() throws IOException
  {
    for(WalFileMeta file : files.values())
    {
      bfs.delete(bucketKey, "WAL-" + file.walId);
    }
    files = Maps.newTreeMap();
  }

  /* Remove old WAL files, and their metadata */
  public void cleanup(long cleanLsn) throws IOException
  {
    for (WalFileMeta file : files.values()) {

      /* Do not touch current WAL file */
      if (file.walId == walFileId)
        continue;

      /* Remove old file from WAL */
      if (file.walSequenceStart < cleanLsn) {
        files.remove(file.walId);
        bfs.delete(bucketKey, "WAL-" + file.walId);
      }
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


  /* For debugging purpose */
  @SuppressWarnings("unused")
  private void printMeta(int id)
  {
    for(WalFileMeta meta : files.values()) {
      logger.debug("{} FileMeata {}", id, meta);
      for(Map.Entry<Long, OffsetRange> widx : meta.windowIndex.entrySet())
        logger.debug(id + "read file " + meta.walId + " lsn " + widx.getKey() + " start " + widx.getValue().start + " end " + widx.getValue().end);
    }
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

  private static transient final Logger logger = LoggerFactory.getLogger(BucketWalWriter.class);
}


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.state.managed;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fileaccess.FileAccess;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

import com.datatorrent.netlet.util.Slice;

/**
 * Persists bucket data on disk and maintains meta information about the buckets.
 * <p/>
 *
 * Each bucket has a meta-data file and the format of that is :<br/>
 * <ol>
 * <li>version of the meta data (int)</li>
 * <li>total number of time-buckets (int)</li>
 * <li>For each time bucket
 * <ol>
 * <li>time bucket key (long)</li>
 * <li>size of data (sum of bytes) (long)</li>
 * <li>last transferred window id (long)</li>
 * <li>length of the first key in the time-bucket file (int)</li>
 * <li>first key in the time-bucket file (byte[])</li>
 * </ol>
 * </li>
 * </ol>
 * <p/>
 * Meta data information is updated by {@link IncrementalCheckpointManager}. Any updates are restricted to the package.
 *
 * @since 3.4.0
 */
public class BucketsFileSystem implements ManagedStateComponent
{
  static final String META_FILE_NAME = "_META";
  private static final int META_FILE_VERSION = 1;

  private final transient TreeBasedTable<Long, Long, MutableTimeBucketMeta> timeBucketsMeta = TreeBasedTable.create();

  //Check-pointed set of all buckets this instance has written to.
  protected final Set<Long> bucketNamesOnFS = new ConcurrentSkipListSet<>();

  protected transient ManagedStateContext managedStateContext;

  @Override
  public void setup(@NotNull ManagedStateContext managedStateContext)
  {
    this.managedStateContext = Preconditions.checkNotNull(managedStateContext, "managed state context");
  }

  protected FileAccess.FileWriter getWriter(long bucketId, String fileName) throws IOException
  {
    return managedStateContext.getFileAccess().getWriter(bucketId, fileName);
  }

  protected FileAccess.FileReader getReader(long bucketId, String fileName) throws IOException
  {
    return managedStateContext.getFileAccess().getReader(bucketId, fileName);
  }

  protected void rename(long bucketId, String fromName, String toName) throws IOException
  {
    managedStateContext.getFileAccess().rename(bucketId, fromName, toName);
  }

  protected DataOutputStream getOutputStream(long bucketId, String fileName) throws IOException
  {
    return managedStateContext.getFileAccess().getOutputStream(bucketId, fileName);
  }

  protected DataInputStream getInputStream(long bucketId, String fileName) throws IOException
  {
    return managedStateContext.getFileAccess().getInputStream(bucketId, fileName);
  }

  protected boolean exists(long bucketId, String fileName) throws IOException
  {
    return managedStateContext.getFileAccess().exists(bucketId, fileName);
  }

  protected RemoteIterator<LocatedFileStatus> listFiles(long bucketId) throws IOException
  {
    return managedStateContext.getFileAccess().listFiles(bucketId);
  }

  protected void delete(long bucketId, String fileName) throws IOException
  {
    managedStateContext.getFileAccess().delete(bucketId, fileName);
  }

  protected void deleteBucket(long bucketId) throws IOException
  {
    managedStateContext.getFileAccess().deleteBucket(bucketId);
  }

  /**
   * Saves data to a bucket. The data consists of key/values of all time-buckets of a particular bucket.
   *
   * @param windowId        window id
   * @param bucketId        bucket id
   * @param data            data of all time-buckets
   * @throws IOException
   */
  protected void writeBucketData(long windowId, long bucketId, Map<Slice, Bucket.BucketedValue> data,  long latestPurgedTimeBucket) throws IOException
  {
    Table<Long, Slice, Bucket.BucketedValue> timeBucketedKeys = TreeBasedTable.create(Ordering.<Long>natural(),
        managedStateContext.getKeyComparator());

    for (Map.Entry<Slice, Bucket.BucketedValue> entry : data.entrySet()) {
      long timeBucketId = entry.getValue().getTimeBucket();
      if (timeBucketId <= latestPurgedTimeBucket) {
        continue;
      }
      timeBucketedKeys.put(timeBucketId, entry.getKey(), entry.getValue());
    }

    for (long timeBucket : timeBucketedKeys.rowKeySet()) {
      BucketsFileSystem.MutableTimeBucketMeta tbm = getMutableTimeBucketMeta(bucketId, timeBucket);
      if (tbm == null) {
        tbm = new MutableTimeBucketMeta(bucketId, timeBucket);
      }

      addBucketName(bucketId);

      long dataSize = 0;
      Slice firstKey = null;

      FileAccess.FileWriter fileWriter;
      String tmpFileName = getTmpFileName();
      if (tbm.getLastTransferredWindowId() == -1) {
        //A new time bucket so we append all the key/values to the new file
        fileWriter = getWriter(bucketId, tmpFileName);

        for (Map.Entry<Slice, Bucket.BucketedValue> entry : timeBucketedKeys.row(timeBucket).entrySet()) {
          Slice key = entry.getKey();
          Slice value = entry.getValue().getValue();

          dataSize += key.length;
          dataSize += value.length;

          fileWriter.append(key, value);
          if (firstKey == null) {
            firstKey = key;
          }
        }
      } else {
        //the time bucket existed so we need to read the file and then re-write it
        TreeMap<Slice, Slice> fileData = new TreeMap<>(managedStateContext.getKeyComparator());
        FileAccess.FileReader fileReader = getReader(bucketId, getFileName(timeBucket));
        fileReader.readFully(fileData);
        fileReader.close();

        for (Map.Entry<Slice, Bucket.BucketedValue> entry : timeBucketedKeys.row(timeBucket).entrySet()) {
          fileData.put(entry.getKey(), entry.getValue().getValue());
        }

        fileWriter = getWriter(bucketId, tmpFileName);
        for (Map.Entry<Slice, Slice> entry : fileData.entrySet()) {
          Slice key = entry.getKey();
          Slice value = entry.getValue();

          dataSize += key.length;
          dataSize += value.length;

          fileWriter.append(key, value);
          if (firstKey == null) {
            firstKey = key;
          }
        }
      }
      fileWriter.close();
      rename(bucketId, tmpFileName, getFileName(timeBucket));
      tbm.updateTimeBucketMeta(windowId, dataSize, firstKey);
      updateTimeBuckets(tbm);
    }

    updateBucketMetaFile(bucketId);
  }

  /**
   * Retrieves the time bucket meta of a particular time-bucket. If the time bucket doesn't exist then a new one
   * is created.
   *
   * @param bucketId     bucket id
   * @param timeBucketId time bucket id
   * @return time bucket meta of the time bucket
   * @throws IOException
   */
  @NotNull
  private MutableTimeBucketMeta getMutableTimeBucketMeta(long bucketId, long timeBucketId) throws IOException
  {
    synchronized (timeBucketsMeta) {
      return timeBucketMetaHelper(bucketId, timeBucketId);
    }
  }

  void updateTimeBuckets(@NotNull MutableTimeBucketMeta mutableTimeBucketMeta)
  {
    Preconditions.checkNotNull(mutableTimeBucketMeta, "mutable time bucket meta");
    synchronized (timeBucketsMeta) {
      timeBucketsMeta.put(mutableTimeBucketMeta.getBucketId(), mutableTimeBucketMeta.getTimeBucketId(),
          mutableTimeBucketMeta);
    }
  }

  protected void addBucketName(long bucketId)
  {
    bucketNamesOnFS.add(bucketId);
  }

  /**
   * Returns the time bucket meta of a particular time-bucket which is immutable.
   *
   * @param bucketId     bucket id
   * @param timeBucketId time bucket id
   * @return immutable time bucket meta
   * @throws IOException
   */
  @Nullable
  public TimeBucketMeta getTimeBucketMeta(long bucketId, long timeBucketId) throws IOException
  {
    synchronized (timeBucketsMeta) {
      MutableTimeBucketMeta tbm = timeBucketMetaHelper(bucketId, timeBucketId);
      if (tbm != null) {
        return tbm.getImmutableTimeBucketMeta();
      }
      return null;
    }
  }

  /**
   * This should be entered only after acquiring the lock on {@link #timeBucketsMeta}
   *
   * @param bucketId      bucket id
   * @param timeBucketId  time bucket id
   * @return Mutable time bucket meta for a bucket id and time bucket id.
   * @throws IOException
   */
  private MutableTimeBucketMeta timeBucketMetaHelper(long bucketId, long timeBucketId) throws IOException
  {
    if (!timeBucketsMeta.containsRow(bucketId) && exists(bucketId, META_FILE_NAME)) {
      try (DataInputStream dis = getInputStream(bucketId, META_FILE_NAME)) {
        //Load meta info of all the time buckets of the bucket identified by bucketId.
        loadBucketMetaFile(bucketId, dis);
      }
    }
    return timeBucketsMeta.get(bucketId, timeBucketId);
  }

  /**
   * Returns the meta information of all the time buckets in the bucket in descending order - latest to oldest.
   *
   * @param bucketId bucket id
   * @return all the time buckets in order - latest to oldest
   */
  public TreeMap<Long, TimeBucketMeta> getAllTimeBuckets(long bucketId) throws IOException
  {
    synchronized (timeBucketsMeta) {
      TreeMap<Long, TimeBucketMeta> immutableTimeBucketMetas = Maps.newTreeMap(Ordering.natural().<Long>reverse());

      if (timeBucketsMeta.containsRow(bucketId)) {
        for (Map.Entry<Long, MutableTimeBucketMeta> entry : timeBucketsMeta.row(bucketId).entrySet()) {
          immutableTimeBucketMetas.put(entry.getKey(), entry.getValue().getImmutableTimeBucketMeta());
        }
        return immutableTimeBucketMetas;
      }
      if (exists(bucketId, META_FILE_NAME)) {
        try (DataInputStream dis = getInputStream(bucketId, META_FILE_NAME)) {
          //Load meta info of all the time buckets of the bucket identified by bucket id
          loadBucketMetaFile(bucketId, dis);
          for (Map.Entry<Long, MutableTimeBucketMeta> entry : timeBucketsMeta.row(bucketId).entrySet()) {
            immutableTimeBucketMetas.put(entry.getKey(), entry.getValue().getImmutableTimeBucketMeta());
          }
          return immutableTimeBucketMetas;
        }
      }
      return immutableTimeBucketMetas;
    }
  }

  /**
   * Loads the bucket meta-file. This should be entered only after acquiring the lock on {@link #timeBucketsMeta}.
   *
   * @param bucketId bucket id
   * @param dis      data input stream
   * @throws IOException
   */
  private void loadBucketMetaFile(long bucketId, DataInputStream dis) throws IOException
  {
    LOG.debug("Loading bucket meta-file {}", bucketId);
    int metaDataVersion = dis.readInt();

    if (metaDataVersion == META_FILE_VERSION) {
      int numberOfEntries = dis.readInt();

      for (int i = 0; i < numberOfEntries; i++) {
        long timeBucketId = dis.readLong();
        long dataSize = dis.readLong();
        long lastTransferredWindow = dis.readLong();

        MutableTimeBucketMeta tbm = new MutableTimeBucketMeta(bucketId, timeBucketId);

        int sizeOfFirstKey = dis.readInt();
        byte[] firstKeyBytes = new byte[sizeOfFirstKey];
        dis.readFully(firstKeyBytes, 0, firstKeyBytes.length);
        tbm.updateTimeBucketMeta(lastTransferredWindow, dataSize, new Slice(firstKeyBytes));

        timeBucketsMeta.put(bucketId, timeBucketId, tbm);
      }
    }
  }

  /**
   * Saves the updated bucket meta on disk.
   *
   * @param bucketId bucket id
   * @throws IOException
   */
  void updateBucketMetaFile(long bucketId) throws IOException
  {
    Map<Long, MutableTimeBucketMeta> timeBuckets;
    synchronized (timeBucketsMeta) {
      timeBuckets = timeBucketsMeta.row(bucketId);

      Preconditions.checkNotNull(timeBuckets, "timeBuckets");
      String tmpFileName = getTmpFileName();

      try (DataOutputStream dos = getOutputStream(bucketId, tmpFileName)) {
        dos.writeInt(META_FILE_VERSION);
        dos.writeInt(timeBuckets.size());
        for (Map.Entry<Long, MutableTimeBucketMeta> entry : timeBuckets.entrySet()) {
          MutableTimeBucketMeta tbm = entry.getValue();
          dos.writeLong(tbm.getTimeBucketId());
          dos.writeLong(tbm.getSizeInBytes());
          dos.writeLong(tbm.getLastTransferredWindowId());
          dos.writeInt(tbm.getFirstKey().length);
          dos.write(tbm.getFirstKey().toByteArray());
        }

      }
      rename(bucketId, tmpFileName, META_FILE_NAME);
    }
  }

  protected void deleteTimeBucketsLessThanEqualTo(long latestExpiredTimeBucket) throws IOException
  {
    LOG.debug("delete files before {}", latestExpiredTimeBucket);

    for (long bucketName : bucketNamesOnFS) {
      RemoteIterator<LocatedFileStatus> timeBucketsIterator = listFiles(bucketName);
      boolean emptyBucket = true;
      while (timeBucketsIterator.hasNext()) {
        LocatedFileStatus timeBucketStatus = timeBucketsIterator.next();

        String timeBucketStr = timeBucketStatus.getPath().getName();
        if (timeBucketStr.equals(BucketsFileSystem.META_FILE_NAME) || timeBucketStr.endsWith(".tmp")) {
          //ignoring meta and tmp files
          continue;
        }
        long timeBucket = Long.parseLong(timeBucketStr);

        if (timeBucket <= latestExpiredTimeBucket) {
          LOG.debug("deleting bucket {} time-bucket {}", timeBucket);
          invalidateTimeBucket(bucketName, timeBucket);
          delete(bucketName, timeBucketStatus.getPath().getName());
        } else {
          emptyBucket = false;
        }
      }
      if (emptyBucket) {
        LOG.debug("deleting bucket {}", bucketName);
        deleteBucket(bucketName);
      }
    }
  }

  void invalidateTimeBucket(long bucketId, long timeBucketId) throws IOException
  {
    synchronized (timeBucketsMeta) {
      timeBucketsMeta.remove(bucketId, timeBucketId);
    }
    updateBucketMetaFile(bucketId);
  }

  @Override
  public void teardown()
  {
  }

  /**
   * This serves the readers - {@link Bucket.DefaultBucket}.
   * It is immutable and accessible outside the package unlike {@link MutableTimeBucketMeta}.
   */
  public static class TimeBucketMeta implements Comparable<TimeBucketMeta>
  {
    private final long bucketId;
    private final long timeBucketId;
    private long lastTransferredWindowId = -1;
    private long sizeInBytes;
    private Slice firstKey;

    private TimeBucketMeta()
    {
      //for kryo
      bucketId = -1;
      timeBucketId = -1;
    }

    private TimeBucketMeta(long bucketId, long timeBucketId)
    {
      this.bucketId = bucketId;
      this.timeBucketId = timeBucketId;
    }

    public long getLastTransferredWindowId()
    {
      return lastTransferredWindowId;
    }

    public long getSizeInBytes()
    {
      return this.sizeInBytes;
    }

    public long getBucketId()
    {
      return bucketId;
    }

    public long getTimeBucketId()
    {
      return timeBucketId;
    }

    public Slice getFirstKey()
    {
      return firstKey;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimeBucketMeta)) {
        return false;
      }

      TimeBucketMeta that = (TimeBucketMeta)o;

      return bucketId == that.bucketId && timeBucketId == that.timeBucketId;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(bucketId, timeBucketId);
    }

    @Override
    public int compareTo(@NotNull TimeBucketMeta o)
    {
      if (bucketId < o.bucketId) {
        return -1;
      }
      if (bucketId > o.bucketId) {
        return 1;
      }
      if (timeBucketId < o.timeBucketId) {
        return -1;
      }
      if (timeBucketId > o.timeBucketId) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * Represents time bucket meta information which can be changed.
   * The updates to an instance and read/creation of {@link #immutableTimeBucketMeta} belonging to it are synchronized
   * as different threads are updating and reading from it.<br/>
   *
   * The instance is updated when data from window files are transferred to bucket files and
   * {@link Bucket.DefaultBucket} reads the immutable time bucket meta.
   */
  static class MutableTimeBucketMeta extends TimeBucketMeta
  {
    private transient TimeBucketMeta immutableTimeBucketMeta;

    private volatile boolean changed;

    public MutableTimeBucketMeta(long bucketId, long timeBucketId)
    {
      super(bucketId, timeBucketId);
    }

    synchronized void updateTimeBucketMeta(long lastTransferredWindow, long bytes, @NotNull Slice firstKey)
    {
      changed = true;
      super.lastTransferredWindowId = lastTransferredWindow;
      super.sizeInBytes = bytes;
      super.firstKey = Preconditions.checkNotNull(firstKey, "first key");
    }

    synchronized TimeBucketMeta getImmutableTimeBucketMeta()
    {
      if (immutableTimeBucketMeta == null || changed) {

        immutableTimeBucketMeta = new TimeBucketMeta(getBucketId(), getTimeBucketId());
        immutableTimeBucketMeta.lastTransferredWindowId = getLastTransferredWindowId();
        immutableTimeBucketMeta.sizeInBytes = getSizeInBytes();
        immutableTimeBucketMeta.firstKey = getFirstKey();
        changed = false;
      }
      return immutableTimeBucketMeta;
    }

  }

  protected static String getFileName(long timeBucketId)
  {
    return Long.toString(timeBucketId);
  }

  protected static String getTmpFileName()
  {
    return System.currentTimeMillis() + ".tmp";
  }

  private static final Logger LOG = LoggerFactory.getLogger(BucketsFileSystem.class);

}

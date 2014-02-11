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
package com.datatorrent.lib.bucket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * {@link BucketStore} which works with HDFS.
 *
 * @param <T> type of bucket event</T>
 */
public class HdfsBucketStore<T extends BucketEvent> implements BucketStore<T>
{
  static transient final String PATH_SEPARATOR = "/";
  static transient final String BUCKETS_SUBDIR = "buckets";
  private transient final String bucketFileSystemPath;
  private transient Configuration configuration;
  private transient Kryo serde;
  private transient Serializer<Map<Object, T>> bucketDataSerializer;
  private transient int operatorId;
  private transient final Set<Integer> partitionKeys;
  private transient int partitionMask;
  private transient final int maxNoOfBucketsInDir;
  private transient int divisor;
  private transient int memoizedBucketIdx;
  private transient String memoizedPath;
  private transient long committedWindowOfLastRun;

  /**
   * Constructs a hdfs store.
   *
   * @param applicationPath     application path.
   * @param operatorId          operator id.
   * @param maxNoOfBucketsInDir maximum no. of buckets (files) which are created in a directory. When the count of buckets
   *                            is greater than this number there are sub-directories created.
   * @param partitionKeys       partitions of the operator.
   * @param partitionMask       partition mask.
   */
  public HdfsBucketStore(String applicationPath, int operatorId, int maxNoOfBucketsInDir, Set<Integer> partitionKeys,
                         int partitionMask)
  {
    this.bucketFileSystemPath = applicationPath + PATH_SEPARATOR + BUCKETS_SUBDIR;
    this.operatorId = operatorId;
    this.maxNoOfBucketsInDir = maxNoOfBucketsInDir;
    this.partitionKeys = Preconditions.checkNotNull(partitionKeys, "partition keys");
    this.partitionMask = partitionMask;
    memoizedBucketIdx = -1;
    memoizedPath = null;
    divisor = 1;
    committedWindowOfLastRun = -1;
  }

  private String getParentBucketPath(int bucketIdx)
  {
    if (memoizedBucketIdx == bucketIdx) {
      return memoizedPath;
    }
    StringBuilder pathBuilder = new StringBuilder();
    int ldivisor = divisor;
    while (ldivisor >= maxNoOfBucketsInDir) {
      pathBuilder.append(bucketIdx / ldivisor).append(PATH_SEPARATOR);
      ldivisor = ldivisor / maxNoOfBucketsInDir;
    }
    memoizedBucketIdx = bucketIdx;
    return memoizedPath = pathBuilder.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setup(int noOfBuckets, final long committedWindowOfLastRun, boolean writeEventKeysOnly) throws IOException
  {
    this.committedWindowOfLastRun = committedWindowOfLastRun;
    this.configuration = new Configuration();
    this.serde = new Kryo();
    this.serde.setClassLoader(Thread.currentThread().getContextClassLoader());
    this.serde.setReferences(false);
    try {
      this.bucketDataSerializer = new BucketDataSerializer<T>(writeEventKeysOnly, partitionKeys, partitionMask);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    double temp = noOfBuckets;
    while (temp > maxNoOfBucketsInDir) {
      divisor = maxNoOfBucketsInDir * divisor;
      temp = Math.ceil(temp / maxNoOfBucketsInDir);
    }
    logger.debug("operator parameters {}, {}, {}", operatorId, partitionKeys, partitionMask);
    logger.debug("dir parameters {}", divisor);

    String lParentPath = null;
    Path rootBucketPath = new Path(bucketFileSystemPath);
    FileSystem fs = FileSystem.get(rootBucketPath.toUri(), configuration);
    if (fs.exists(rootBucketPath)) {
      for (int bucketIdx = 0; bucketIdx < noOfBuckets; bucketIdx++) {
        if (lParentPath == null) {
          lParentPath = getParentBucketPath(bucketIdx);
        }

        //Delete bucket data which belongs to windows greater than committed window of last run
        Path bucketPath = new Path(bucketFileSystemPath + PATH_SEPARATOR + lParentPath + bucketIdx);
        if (fs.exists(bucketPath)) {
          FileStatus[] statusForDeletion = fs.listStatus(bucketPath, new PathFilter()
          {
            @Override
            public boolean accept(Path path)
            {
              try {
                long window = Long.parseLong(path.getName());
                return window > committedWindowOfLastRun;
              }
              catch (NumberFormatException nfe) {
                logger.warn("invalid window {}", committedWindowOfLastRun, nfe);
                return false;
              }
            }
          });

          for (FileStatus fileStatus : statusForDeletion) {
            fs.delete(fileStatus.getPath(), true);
          }

          if (bucketIdx % maxNoOfBucketsInDir == maxNoOfBucketsInDir - 1) {
            lParentPath = null;
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void teardown()
  {
    configuration.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void storeBucketData(int bucketIdx, long window, Map<Object, T> recentData) throws IOException
  {
    OutputStream stream = getSaveStream(bucketIdx, window);
    if (stream != null) {
      Output output = new Output(stream);
      serde.writeObject(output, recentData, bucketDataSerializer);
      output.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteBucket(int bucketIdx) throws IOException
  {
    Path bucketPath = new Path(bucketFileSystemPath + PATH_SEPARATOR + getParentBucketPath(bucketIdx) + bucketIdx);
    FileSystem fs = FileSystem.get(bucketPath.toUri(), configuration);
    fs.delete(bucketPath, true);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public Map<Object, T> fetchBucket(int bucketIdx) throws Exception
  {
    Map<Object, T> bucketData = Maps.newHashMap();

    Path bucketPath = new Path(bucketFileSystemPath + PATH_SEPARATOR + getParentBucketPath(bucketIdx) + bucketIdx);
    FileSystem fs = FileSystem.get(bucketPath.toUri(), configuration);
    if (!fs.exists(bucketPath)) {
      return bucketData;
    }

    List<FileStatus> windowsOfPreviousRun = Lists.newArrayList();
    List<FileStatus> windowsOfCurrentRun = Lists.newArrayList();

    for (FileStatus windowDir : fs.listStatus(bucketPath)) {
      long window = Long.parseLong(windowDir.getPath().getName());

      if (window > committedWindowOfLastRun) {
        windowsOfCurrentRun.add(windowDir);
      }
      else {
        windowsOfPreviousRun.add(windowDir);
      }
    }
    for (FileStatus previousRunWindow : windowsOfPreviousRun) {
      for (FileStatus operatorFiles : fs.listStatus(previousRunWindow.getPath())) {
        InputStream bucketStream = getLoadStream(operatorFiles.getPath());
        Input input = new Input(bucketStream);
        @SuppressWarnings("unchecked")
        Map<Object, T> bucketDataPerWindow = serde.readObject(input, bucketData.getClass(), bucketDataSerializer);
        bucketData.putAll(bucketDataPerWindow);
      }
    }
    for (FileStatus currentRunWindow : windowsOfCurrentRun) {
      Path operatorFile = new Path(currentRunWindow.getPath().toString() + PATH_SEPARATOR + operatorId);
      if (fs.exists(operatorFile)) {
        InputStream bucketStream = getLoadStream(operatorFile);
        Input input = new Input(bucketStream);
        @SuppressWarnings("unchecked")
        Map<Object, T> bucketDataPerWindow = serde.readObject(input, bucketData.getClass(), bucketDataSerializer);
        bucketData.putAll(bucketDataPerWindow);
      }
    }

    return bucketData;
  }

  private OutputStream getSaveStream(int bucketIdx, long window) throws IOException
  {
    Path path = new Path(bucketFileSystemPath + PATH_SEPARATOR + getParentBucketPath(bucketIdx) + bucketIdx +
      PATH_SEPARATOR + window + PATH_SEPARATOR + operatorId);
    FileSystem fs = FileSystem.get(path.toUri(), configuration);
    return fs.create(path);
  }

  private InputStream getLoadStream(Path bucketWindowPath) throws IOException
  {
    FileSystem fs = FileSystem.get(bucketWindowPath.toUri(), configuration);
    return fs.open(bucketWindowPath);
  }

  public static class BucketDataSerializer<T extends BucketEvent> extends Serializer<Map<Object, T>>
  {
    private final Output temporaryOutput;
    private final boolean writeEventKeysOnly;
    private final Set<Integer> partitionKeys;
    private final int partitionMask;

    public BucketDataSerializer(boolean writeEventKeysOnly, Set<Integer> partitionKeys, int partitionMask) throws IOException
    {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      temporaryOutput = new Output(byteStream);
      this.writeEventKeysOnly = writeEventKeysOnly;
      this.partitionKeys = Preconditions.checkNotNull(partitionKeys, "partition keys");
      this.partitionMask = partitionMask;
    }

    @Override
    public void write(Kryo kryo, Output output, Map<Object, T> bucketData)
    {
      if (bucketData.isEmpty()) {
        return;
      }

      kryo.writeClass(output, bucketData.entrySet().iterator().next().getKey().getClass());
      if (!writeEventKeysOnly) {
        kryo.writeClass(output, bucketData.entrySet().iterator().next().getValue().getClass());
      }

      output.writeInt(bucketData.size(), true);

      for (Map.Entry<Object, T> entry : bucketData.entrySet()) {
        kryo.writeObject(output, entry.getKey());
        if (!writeEventKeysOnly) {
          kryo.writeObject(temporaryOutput, entry.getValue());
          byte[] valueInBytes = temporaryOutput.toBytes();
          output.writeInt(valueInBytes.length, true);
          output.writeBytes(valueInBytes);
          temporaryOutput.clear();
        }
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Object, T> read(Kryo kryo, Input input, Class<Map<Object, T>> bucketDataClass)
    {
      Map<Object, T> bucketData = Maps.newHashMap();
      Class<Object> keyClass = kryo.readClass(input).getType();
      Class<T> valueClass = null;
      if (!writeEventKeysOnly) {
        valueClass = kryo.readClass(input).getType();
      }

      int length = input.readInt(true);

      for (int i = 0; i < length; i++) {
        Object key = kryo.readObject(input, keyClass);
        int partitionKey = key.hashCode() & partitionMask;
        boolean keyPasses = partitionKeys.contains(partitionKey);
        if (!writeEventKeysOnly) {
          //if key passes then read the value otherwise skip the value
          int entrySize = input.readInt(true);
          if (keyPasses) {
            T entry = kryo.readObject(input, valueClass);
            bucketData.put(key, entry);
          }
          else {
            input.skip(entrySize);
          }
        }
        else if (keyPasses) {
          bucketData.put(key, null);
        }
      }
      return bucketData;
    }
  }

  private static transient final Logger logger = LoggerFactory.getLogger(HdfsBucketStore.class);
}

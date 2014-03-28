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

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * {@link BucketStore} which works with HDFS.<br/>
 * The path of buckets in hdfs is <code>{application-path}/buckets/{operatorId}/{windowId}</code>.
 *
 * @param <T> type of bucket event
 */
public class HdfsBucketStore<T extends Bucketable> implements BucketStore<T>
{
  public static transient String OPERATOR_ID = "operatorId";
  public static transient String APP_PATH = "applicationPath";
  public static transient String PARTITION_KEYS = "partitionKeys";
  public static transient String PARTITION_MASK = "partitionMask";

  static transient final String PATH_SEPARATOR = "/";
  static transient final String BUCKETS_SUBDIR = "buckets";

  //Check-pointed
  private boolean writeEventKeysOnly;
  @Min(1)
  protected int noOfBuckets;
  private Map<Long, Long>[] bucketPositions;
  private Class<Object> eventKeyClass;
  private Class<T> eventClass;

  //Non check-pointed
  private transient FileSystem fs;
  private transient String bucketRoot;
  private transient Configuration configuration;
  private transient Kryo serde;
  private transient Set<Integer> partitionKeys;
  private transient int partitionMask;
  private transient long committedWindowOfLastRun;

  public HdfsBucketStore()
  {
    //Used for kryo serialization
    committedWindowOfLastRun = -1;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setNoOfBuckets(int noOfBuckets)
  {
    this.noOfBuckets = noOfBuckets;
    bucketPositions = (Map<Long, Long>[]) Array.newInstance(HashMap.class, noOfBuckets);
  }

  @Override
  public void setWriteEventKeysOnly(boolean writeEventKeysOnly)
  {
    this.writeEventKeysOnly = writeEventKeysOnly;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public void setup(Context context)
  {
    int operatorId = Preconditions.checkNotNull(context.getInt(OPERATOR_ID, null));
    this.bucketRoot = Preconditions.checkNotNull(context.getString(APP_PATH, null), "app path") + PATH_SEPARATOR + BUCKETS_SUBDIR + PATH_SEPARATOR + operatorId;
    this.partitionKeys = (Set<Integer>) Preconditions.checkNotNull(context.getObject(PARTITION_KEYS, null), "partition keys");
    this.partitionMask = Preconditions.checkNotNull(context.getInt(PARTITION_MASK, null), "partition mask");
    logger.debug("operator parameters {}, {}, {}, {}", operatorId, partitionKeys, partitionMask, committedWindowOfLastRun);

    this.configuration = new Configuration();
    this.serde = new Kryo();
    this.serde.setClassLoader(Thread.currentThread().getContextClassLoader());
    try {
      this.fs = FileSystem.get(new Path(bucketRoot).toUri(), configuration);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (logger.isDebugEnabled()) {
      for (int i = 0; i < bucketPositions.length; i++) {
        if (bucketPositions[i] != null) {
          logger.debug("bucket idx {} position {}", i, bucketPositions[i]);
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
  @SuppressWarnings("unchecked")
  @Override
  public void storeBucketData(long window, Map<Integer, Map<Object, T>> data) throws IOException
  {
    Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + window);
    FSDataOutputStream dataStream = new FSDataOutputStream(fs.create(dataFilePath), null);

    Output output = new Output(dataStream);
    long offset = 0;
    for (int bucketIdx : data.keySet()) {
      if (bucketPositions[bucketIdx] == null) {
        bucketPositions[bucketIdx] = Maps.newHashMap();
      }
      bucketPositions[bucketIdx].put(window, offset);

      Map<Object, T> bucketData = data.get(bucketIdx);
      if (eventKeyClass == null) {
        Map.Entry<Object, T> eventEntry = bucketData.entrySet().iterator().next();
        eventKeyClass = (Class<Object>) eventEntry.getKey().getClass();

        if (!writeEventKeysOnly) {
          eventClass = (Class<T>) eventEntry.getValue().getClass();
        }
      }
      //Write the size of data and then data
      dataStream.writeInt(bucketData.size());
      for (Map.Entry<Object, T> entry : bucketData.entrySet()) {
        serde.writeObject(output, entry.getKey());

        if (!writeEventKeysOnly) {
          int numValuePos = output.position();
          output.writeInt(0); //temporary place holder
          serde.writeObject(output, entry.getValue());
          int valueLength = output.position() - numValuePos - 4;
          output.setPosition(numValuePos);
          output.writeInt(valueLength);
        }
      }
      output.flush();
      offset = dataStream.getPos();

    }
    output.close();
    dataStream.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteBucket(int bucketIdx) throws IOException
  {
    bucketPositions[bucketIdx] = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public Map<Object, T> fetchBucket(int bucketIdx) throws Exception
  {
    Map<Object, T> bucketData = Maps.newHashMap();

    if (bucketPositions[bucketIdx] == null) {
      return bucketData;
    }

    for (long window : bucketPositions[bucketIdx].keySet()) {

      //Read data only for the windows in which bucketIdx had events.
      Path dataFile = new Path(bucketRoot + PATH_SEPARATOR + window);
      FSDataInputStream stream = new FSDataInputStream(fs.open(dataFile));
      stream.seek(bucketPositions[bucketIdx].get(window));

      Input input = new Input(stream);
      int length = stream.readInt();

      for (int i = 0; i < length; i++) {
        Object key = serde.readObject(input, eventKeyClass);

        int partitionKey = key.hashCode() & partitionMask;
        boolean keyPasses = partitionKeys.contains(partitionKey);

        if (!writeEventKeysOnly) {
          //if key passes then read the value otherwise skip the value
          int entrySize = input.readInt();
          if (keyPasses) {
            T entry = serde.readObject(input, eventClass);
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
      input.close();
      stream.close();
    }
    return bucketData;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HdfsBucketStore)) {
      return false;
    }

    HdfsBucketStore that = (HdfsBucketStore) o;

    if (noOfBuckets != that.noOfBuckets) {
      return false;
    }
    if (writeEventKeysOnly != that.writeEventKeysOnly) {
      return false;
    }
    return Arrays.equals(bucketPositions, that.bucketPositions);

  }

  @Override
  public int hashCode()
  {
    int result = (writeEventKeysOnly ? 1 : 0);
    result = 31 * result + noOfBuckets;
    result = 31 * result + (bucketPositions != null ? Arrays.hashCode(bucketPositions) : 0);
    return result;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(HdfsBucketStore.class);
}

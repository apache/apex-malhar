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
package org.apache.apex.malhar.lib.utils.serde;

import org.apache.apex.malhar.lib.state.managed.Bucket;
import org.apache.apex.malhar.lib.state.managed.BucketProvider;

import com.datatorrent.netlet.util.Slice;

/**
 * @since 3.6.0
 */
public class KeyValueSerdeManager<K, V>
{
  public static final long INVALID_BUCKET_ID = -1;
  protected Serde<K> keySerde;
  protected Serde<V> valueSerde;

  protected SerializationBuffer keyBufferForWrite;
  protected transient SerializationBuffer keyBufferForRead = SerializationBuffer.READ_BUFFER;

  protected SerializationBuffer valueBuffer;

  private long lastBucketId = INVALID_BUCKET_ID;
  private transient BucketProvider bucketProvider;

  protected KeyValueSerdeManager()
  {
    //for kyro
  }

  public KeyValueSerdeManager(Serde<K> keySerde, Serde<V> valueSerde)
  {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  public void setup(BucketProvider bp, long bucketId)
  {
    bucketProvider = bp;
    updateBuffersForBucketChange(bucketId);
  }

  /**
   * The bucket can be changed. The write buffer should also be changed if bucket changed.
   * @param bucketId
   */
  public void updateBuffersForBucketChange(long bucketId)
  {
    if (lastBucketId == bucketId) {
      return;
    }

    Bucket bucketInst = bucketProvider.ensureBucket(bucketId);
    this.valueBuffer = new SerializationBuffer(bucketInst.getValueStream());
    keyBufferForWrite = new SerializationBuffer(bucketInst.getKeyStream());

    lastBucketId = bucketId;
  }


  public Slice serializeKey(K key, boolean write)
  {
    SerializationBuffer buffer = write ? keyBufferForWrite : keyBufferForRead;
    keySerde.serialize(key, buffer);
    return buffer.toSlice();
  }


  /**
   * Value only serialize for writing
   * @param value
   * @return
   */
  public Slice serializeValue(V value)
  {
    valueSerde.serialize(value, valueBuffer);
    return valueBuffer.toSlice();
  }

  public void beginWindow(long windowId)
  {
    keyBufferForWrite.beginWindow(windowId);
    valueBuffer.beginWindow(windowId);
  }

  public void resetReadBuffer()
  {
    keyBufferForRead.release();
  }
}

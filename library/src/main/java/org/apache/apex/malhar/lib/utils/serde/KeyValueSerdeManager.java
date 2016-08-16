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

public class KeyValueSerdeManager<K, V>
{
  protected Serde<K> keySerde;
  protected Serde<V> valueSerde;

  protected SerializationBuffer keyBufferForWrite;
  protected transient SerializationBuffer keyBufferForRead = DefaultSerializationBuffer.READ_BUFFER;

  protected SerializationBuffer valueBuffer;


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
    //the bucket will not change for this class. so get streams from setup, else, need to set stream before serialize
    Bucket bucketInst = bp.ensureBucket(bucketId);
    this.valueBuffer = new DefaultSerializationBuffer(bucketInst.getValueStream());

    keyBufferForWrite = new DefaultSerializationBuffer(bucketInst.getKeyStream());
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

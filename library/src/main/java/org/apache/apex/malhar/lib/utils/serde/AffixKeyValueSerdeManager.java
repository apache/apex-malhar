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

import com.esotericsoftware.kryo.io.Input;

import com.datatorrent.netlet.util.Slice;

/**
 * All spillable data structures use this class to manage the buffers for serialization.
 * This class contains serialization logic that is common for all spillable data structures
 *
 * @param <K>
 * @param <V>
 *
 * @since 3.6.0
 */
public class AffixKeyValueSerdeManager<K, V> extends KeyValueSerdeManager<K, V>
{
  /**
   * The read buffer will be released when read is done, while write buffer should be held until the data has been persisted.
   * The write buffer should be non-transient. The data which has been already saved to files will be removed by {@link Bucket}
   * while the data which haven't been saved need to be recovered by the platform from checkpoint.
   */
  private AffixSerde<K> metaKeySerde;
  private AffixSerde<K> dataKeySerde;


  private AffixKeyValueSerdeManager()
  {
    //for kyro
  }

  public AffixKeyValueSerdeManager(byte[] metaKeySuffix, byte[] dataKeyIdentifier, Serde<K> keySerde, Serde<V> valueSerde)
  {
    this.valueSerde = valueSerde;
    metaKeySerde = new AffixSerde<K>(null, keySerde, metaKeySuffix);
    dataKeySerde = new AffixSerde<K>(dataKeyIdentifier, keySerde, null);
  }

  public Slice serializeMetaKey(K key, boolean write)
  {
    SerializationBuffer buffer = write ? keyBufferForWrite : keyBufferForRead;
    metaKeySerde.serialize(key, buffer);
    return buffer.toSlice();
  }

  public Slice serializeDataKey(K key, boolean write)
  {
    SerializationBuffer buffer = write ? keyBufferForWrite : keyBufferForRead;
    dataKeySerde.serialize(key, buffer);
    return buffer.toSlice();
  }

  public V deserializeValue(Input input)
  {
    V value = valueSerde.deserialize(input);
    return value;
  }
}

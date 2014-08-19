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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.KeyValPair;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

public class HDSOperator extends HDSBucketManager implements Partitioner<HDSOperator>
{
  private static final Logger LOG = LoggerFactory.getLogger(HDSOperator.class);

  /**
   * Partition keys identify assigned bucket(s).
   */
  protected int partitionMask;
  protected Set<Integer> partitions;

  private transient StreamCodec<KeyValPair<byte[], byte[]>> streamCodec;

  public final transient DefaultInputPort<KeyValPair<byte[], byte[]>> data = new DefaultInputPort<KeyValPair<byte[], byte[]>>() {
    @Override
    public void process(KeyValPair<byte[], byte[]> tuple)
    {
      int bucketKey = streamCodec.getPartition(tuple) & partitionMask;
      try {
        HDSOperator.this.put(bucketKey, tuple.getKey(), tuple.getValue());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Class<? extends StreamCodec<KeyValPair<byte[], byte[]>>> getStreamCodec()
    {
      return getBucketKeyStreamCodec();
    }
  };

  public static class BucketKeyStreamCodec extends KryoSerializableStreamCodec<KeyValPair<byte[], byte[]>>
  {
    @Override
    public int getPartition(KeyValPair<byte[], byte[]> t)
    {
      int length = t.getKey().length;
      int hash = 0;
      for (int i = 0; i < 4 && i < length; i++) {
        hash = hash << 8;
        hash += t.getKey()[i];
      }
      return hash;
    }
  }

  protected Class<? extends StreamCodec<KeyValPair<byte[], byte[]>>> getBucketKeyStreamCodec()
  {
    return BucketKeyStreamCodec.class;
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    LOG.debug("Opening store {} for partitions {} {}", super.getFileStore(), new PartitionKeys(this.partitionMask, this.partitions));
    super.setup(arg0);
    try {
      this.streamCodec = getBucketKeyStreamCodec().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create streamCodec", e);
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
  }

  @Override
  public Collection<Partition<HDSOperator>> definePartitions(Collection<Partition<HDSOperator>> partitions, int incrementalCapacity)
  {
    boolean isInitialPartition = partitions.iterator().next().getStats() == null;

    if (!isInitialPartition) {
      // support for dynamic partitioning requires lineage tracking
      LOG.warn("Dynamic partitioning not implemented");
      return partitions;
    }

    int totalCount = partitions.size() + incrementalCapacity;
    Kryo kryo = new Kryo();
    Collection<Partition<HDSOperator>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    for (int i = 0; i < totalCount; i++) {
      // Kryo.copy fails as it attempts to clone transient fields (input port)
      // TestStoreOperator oper = kryo.copy(this);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input input = new Input(bos.toByteArray());
      HDSOperator oper = kryo.readObject(input, this.getClass());
      newPartitions.add(new DefaultPartition<HDSOperator>(oper));
    }

    // assign the partition keys
    DefaultPartition.assignPartitionKeys(newPartitions, data);

    for (Partition<HDSOperator> p : newPartitions) {
      PartitionKeys pks = p.getPartitionKeys().get(data);
      p.getPartitionedInstance().partitionMask = pks.mask;
      p.getPartitionedInstance().partitions = pks.partitions;
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<HDSOperator>> arg0)
  {
  }

}

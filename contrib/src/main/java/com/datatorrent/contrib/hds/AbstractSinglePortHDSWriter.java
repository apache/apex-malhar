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
import java.lang.reflect.Field;
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
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

/**
 * Operator that receives data on port and writes it to the data store.
 * Implements partitioning, maps partition key to the store bucket.
 * The derived class supplies the codec for partitioning and key-value serialization.
 * @param <EVENT>
 */
public abstract class AbstractSinglePortHDSWriter<EVENT> extends HDSBucketManager implements Partitioner<AbstractSinglePortHDSWriter<EVENT>>
{
  public interface HDSCodec<EVENT> extends StreamCodec<EVENT>
  {
    byte[] getKeyBytes(EVENT event);
    byte[] getValueBytes(EVENT event);
    EVENT fromKeyValue(byte[] key, byte[] value);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSinglePortHDSWriter.class);

  protected int partitionMask;

  protected Set<Integer> partitions;

  protected transient HDSCodec<EVENT> codec;


  public final transient DefaultInputPort<EVENT> input = new DefaultInputPort<EVENT>()
  {
    @Override
    public void process(EVENT event)
    {
      try {
        processEvent(event);
      } catch (IOException e) {
        throw new RuntimeException("Error processing " + event, e);
      }
    }

    @Override
    public Class<? extends StreamCodec<EVENT>> getStreamCodec()
    {
      return getCodecClass();
    }
  };

  protected long getBucketKey(EVENT event)
  {
    return (codec.getPartition(event) & partitionMask);
  }

  protected void processEvent(EVENT event) throws IOException
  {
    byte[] key = codec.getKeyBytes(event);
    byte[] value = codec.getValueBytes(event);
    super.put(getBucketKey(event), key, value);
  }

  abstract protected Class<? extends HDSCodec<EVENT>> getCodecClass();

  @Override
  public void setup(OperatorContext arg0)
  {
    LOG.debug("Store {} with partitions {} {}", super.getFileStore(), new PartitionKeys(this.partitionMask, this.partitions));
    super.setup(arg0);
    try {
      this.codec = getCodecClass().newInstance();
      // inject the operator reference, if such field exists
      // TODO: replace with broader solution
      Class<?> cls = this.codec.getClass();
      while (cls != null) {
        for (Field field : cls.getDeclaredFields()) {
          if (field.getType().isAssignableFrom(this.getClass())) {
            field.set(this.codec, this);
          }
        }
        cls = cls.getSuperclass();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create codec", e);
    }
  }

  @Override
  public Collection<Partition<AbstractSinglePortHDSWriter<EVENT>>> definePartitions(Collection<Partition<AbstractSinglePortHDSWriter<EVENT>>> partitions, int incrementalCapacity)
  {
    boolean isInitialPartition = partitions.iterator().next().getStats() == null;

    if (!isInitialPartition) {
      // support for dynamic partitioning requires lineage tracking
      LOG.warn("Dynamic partitioning not implemented");
      return partitions;
    }

    int totalCount = partitions.size() + incrementalCapacity;
    Kryo kryo = new Kryo();
    Collection<Partition<AbstractSinglePortHDSWriter<EVENT>>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    for (int i = 0; i < totalCount; i++) {
      // Kryo.copy fails as it attempts to clone transient fields (input port)
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input input = new Input(bos.toByteArray());
      @SuppressWarnings("unchecked")
      AbstractSinglePortHDSWriter<EVENT> oper = kryo.readObject(input, this.getClass());
      newPartitions.add(new DefaultPartition<AbstractSinglePortHDSWriter<EVENT>>(oper));
    }

    // assign the partition keys
    DefaultPartition.assignPartitionKeys(newPartitions, input);

    for (Partition<AbstractSinglePortHDSWriter<EVENT>> p : newPartitions) {
      PartitionKeys pks = p.getPartitionKeys().get(input);
      p.getPartitionedInstance().partitionMask = pks.mask;
      p.getPartitionedInstance().partitions = pks.partitions;
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractSinglePortHDSWriter<EVENT>>> arg0)
  {
  }

}

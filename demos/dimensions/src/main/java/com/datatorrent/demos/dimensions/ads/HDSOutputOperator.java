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
package com.datatorrent.demos.dimensions.ads;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.HDS;
import com.datatorrent.contrib.hds.HDSWriter;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * DataTorrent HDS Output Operator
 *
 * @displayName HDS Output
 * @category Output
 * @tags hds, output operator
 */
public class HDSOutputOperator extends HDSWriter implements Partitioner<HDSOutputOperator>
{
  private static final Logger LOG = LoggerFactory.getLogger(HDSOutputOperator.class);

  private transient final ByteBuffer valbb = ByteBuffer.allocate(8 * 4);

  private transient final ByteBuffer keybb = ByteBuffer.allocate(8 + 4 * 3);

  protected boolean debug = false;

  protected static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  /**
   * Partition keys identify assigned bucket(s).
   */
  protected int partitionMask;

  protected Set<Integer> partitions;

  private transient StreamCodec<AdInfoAggregateEvent> streamCodec;
  protected final SortedMap<Long, Map<AdInfoAggregateEvent, AdInfoAggregateEvent>> cache = Maps.newTreeMap();

  // TODO: should be aggregation interval count
  private int maxCacheSize = 5;

  private AdInfoAggregator aggregator;

  public int getMaxCacheSize()
  {
    return maxCacheSize;
  }

  public void setMaxCacheSize(int maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  @Override
  public void endWindow()
  {
    int expiredEntries = cache.size() - maxCacheSize;
    while(expiredEntries-- > 0){

      Map<AdInfoAggregateEvent, AdInfoAggregateEvent> vals = cache.remove(cache.firstKey());
      for (Entry<AdInfoAggregateEvent, AdInfoAggregateEvent> en : vals.entrySet()) {
        AdInfoAggregateEvent ai = en.getValue();
        try {
          put(getBucketKey(ai), getKey(ai), getValue(ai));
        } catch (IOException e) {
          LOG.warn("Error putting the value", e);
        }
      }
    }

    super.endWindow();
  }


  protected Slice getKey(AdInfo event)
  {
    if (debug) {
      StringBuilder keyBuilder = new StringBuilder(32);
      keyBuilder.append(sdf.format(new Date(event.timestamp)));
      keyBuilder.append("|publisherId:").append(event.publisherId);
      keyBuilder.append("|advertiserId:").append(event.advertiserId);
      keyBuilder.append("|adUnit:").append(event.adUnit);
      return HDS.SliceExt.toSlice(keyBuilder.toString().getBytes());
    }

    byte[] data = new byte[8 + 4 * 3];
    keybb.rewind();
    keybb.putLong(event.getTimestamp());
    keybb.putInt(event.getPublisherId());
    keybb.putInt(event.getAdvertiserId());
    keybb.putInt(event.getAdUnit());
    keybb.rewind();
    keybb.get(data);
    return HDS.SliceExt.toSlice(data);
  }


  protected byte[] getValue(AdInfoAggregateEvent event)
  {

    if (debug) {
      StringBuilder keyBuilder = new StringBuilder(32);
      keyBuilder.append("|clicks:").append(event.clicks);
      keyBuilder.append("|cost:").append(event.cost);
      keyBuilder.append("|impressions:").append(event.impressions);
      keyBuilder.append("|revenue:").append(event.revenue);
      return keyBuilder.toString().getBytes();
    }
    byte[] data = new byte[8 * 4];
    valbb.rewind();
    valbb.putLong(event.clicks);
    valbb.putDouble(event.cost);
    valbb.putLong(event.impressions);
    valbb.putDouble(event.revenue);
    valbb.rewind();
    valbb.get(data);
    return data;
  }


  /**
   * The input port
   */
  @InputPortFieldAnnotation(name = "in", optional = true)
  public final transient DefaultInputPort<AdInfoAggregateEvent> input = new DefaultInputPort<AdInfoAggregateEvent>()
  {
    @Override
    public void process(AdInfoAggregateEvent adInfo)
    {
      Map<AdInfoAggregateEvent, AdInfoAggregateEvent> valMap = cache.get(adInfo.getTimestamp());
      if (valMap == null) {
        valMap = new HashMap<AdInfoAggregateEvent, AdInfoAggregateEvent>();
        valMap.put(adInfo, adInfo);
        cache.put(adInfo.getTimestamp(), valMap);
      } else {
        AdInfoAggregateEvent val = valMap.get(adInfo);
        if (val == null) {
          valMap.put(adInfo, adInfo);
          return;
        } else {
          aggregator.aggregate(val, adInfo);
        }
      }
    }

    @Override
    public Class<? extends StreamCodec<AdInfoAggregateEvent>> getStreamCodec()
    {
      return getBucketKeyStreamCodec();
    }

  };

  public long getBucketKey(AdInfoAggregateEvent aie)
  {
    return (streamCodec.getPartition(aie) & partitionMask);
  }

  public static class BucketKeyStreamCodec extends KryoSerializableStreamCodec<AdInfoAggregateEvent>
  {
    @Override
    public int getPartition(AdInfoAggregateEvent t)
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + t.adUnit;
      result = prime * result + t.advertiserId;
      result = prime * result + t.publisherId;
      return result;
    }
  }

  protected Class<? extends StreamCodec<AdInfoAggregateEvent>> getBucketKeyStreamCodec()
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
  public Collection<Partition<HDSOutputOperator>> definePartitions(Collection<Partition<HDSOutputOperator>> partitions, int incrementalCapacity)
  {
    boolean isInitialPartition = partitions.iterator().next().getStats() == null;

    if (!isInitialPartition) {
      // support for dynamic partitioning requires lineage tracking
      LOG.warn("Dynamic partitioning not implemented");
      return partitions;
    }

    int totalCount = partitions.size() + incrementalCapacity;
    Kryo kryo = new Kryo();
    Collection<Partition<HDSOutputOperator>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    for (int i = 0; i < totalCount; i++) {
      // Kryo.copy fails as it attempts to clone transient fields (input port)
      // TestStoreOperator oper = kryo.copy(this);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input input = new Input(bos.toByteArray());
      HDSOutputOperator oper = kryo.readObject(input, this.getClass());
      newPartitions.add(new DefaultPartition<HDSOutputOperator>(oper));
    }

    // assign the partition keys
    DefaultPartition.assignPartitionKeys(newPartitions, input);

    for (Partition<HDSOutputOperator> p : newPartitions) {
      PartitionKeys pks = p.getPartitionKeys().get(input);
      p.getPartitionedInstance().partitionMask = pks.mask;
      p.getPartitionedInstance().partitions = pks.partitions;
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<HDSOutputOperator>> arg0)
  {
  }


  public AdInfoAggregator getAggregator()
  {
    return aggregator;
  }


  public void setAggregator(AdInfoAggregator aggregator)
  {
    this.aggregator = aggregator;
  }


  public boolean isDebug()
  {
    return debug;
  }


  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }

}

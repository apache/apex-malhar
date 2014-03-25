/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.statistics;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;

import gnu.trove.map.hash.TCustomHashMap;
import gnu.trove.strategy.HashingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ShipContainingJars;

/**
 *
 * @param <EVENT> - Type of the tuple whose attributes are used to define dimensions.
 */
@ShipContainingJars(classes = {TCustomHashMap.class, HashingStrategy.class})
public class DimensionsComputation<EVENT> implements Operator
{
  private Unifier<EVENT> unifier;

  public void setUnifier(Unifier<EVENT> unifier)
  {
    this.unifier = unifier;
  }

  public final transient DefaultOutputPort<EVENT> output = new DefaultOutputPort<EVENT>()
  {
    @Override
    public Unifier<EVENT> getUnifier()
    {
      if (DimensionsComputation.this.unifier == null) {
        return super.getUnifier();
      }
      else {
        return unifier;
      }
    }
  };

  public final transient DefaultInputPort<EVENT> data = new DefaultInputPort<EVENT>()
  {
    @Override
    public void process(EVENT tuple)
    {
      for (AggregatorMap<EVENT> dimension : aggregatorMaps) {
        dimension.add(tuple);
      }
    }

  };

  public static interface Aggregator<EVENT> extends HashingStrategy<EVENT>
  {
    EVENT getGroup(EVENT src);

    void aggregate(EVENT dest, EVENT src);

  }

  private AggregatorMap<EVENT>[] aggregatorMaps;

  /**
   * Set the dimensions which should each get the tuples going forward.
   * A dimension is specified by the colon separated list of fields names which together forms dimension.
   * Dimesions are separated by comma. This form is chosen so that dimensions can be controlled through
   * properties file as well.
   *
   * @param aggregators
   */
  public void setAggregators(Aggregator<EVENT>[] aggregators)
  {
    @SuppressWarnings("unchecked")
    AggregatorMap<EVENT>[] newInstance = (AggregatorMap<EVENT>[])Array.newInstance(AggregatorMap.class, aggregators.length);
    aggregatorMaps = newInstance;
    for (int i = aggregators.length; i-- > 0;) {
      aggregatorMaps[i] = new AggregatorMap<EVENT>(aggregators[i]);
    }
  }

  public Aggregator<EVENT>[] getAggregators()
  {
    @SuppressWarnings("unchecked")
    Aggregator<EVENT>[] aggregators = (Aggregator<EVENT>[])Array.newInstance(Aggregator.class, aggregatorMaps.length);
    for (int i = aggregatorMaps.length; i-- > 0;) {
      aggregators[i] = aggregatorMaps[i].aggregator;
    }
    return aggregators;
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    for (AggregatorMap<EVENT> dimension : aggregatorMaps) {
      for (EVENT value : dimension.values()) {
        output.emit(value);
      }
      dimension.clear();
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public void transferDimension(Aggregator<EVENT> aggregator, DimensionsComputation<EVENT> other)
  {
    if (other.aggregatorMaps == null) {
      if (this.aggregatorMaps == null) {
        @SuppressWarnings("unchecked")
        AggregatorMap<EVENT>[] newInstance = (AggregatorMap<EVENT>[])Array.newInstance(AggregatorMap.class, 1);
        this.aggregatorMaps = newInstance;
      }
      else {
        this.aggregatorMaps = Arrays.copyOf(this.aggregatorMaps, this.aggregatorMaps.length + 1);
      }

      this.aggregatorMaps[this.aggregatorMaps.length - 1] = new AggregatorMap<EVENT>(aggregator);
    }
    else {
      int i = other.aggregatorMaps.length;
      while (i-- > 0) {
        AggregatorMap<EVENT> otherMap = other.aggregatorMaps[i];
        if (aggregator.equals(otherMap.aggregator)) {
          other.aggregatorMaps[i] = null;
          @SuppressWarnings("unchecked")
          AggregatorMap<EVENT>[] newArray = (AggregatorMap<EVENT>[])Array.newInstance(AggregatorMap.class, other.aggregatorMaps.length - 1);

          i = 0;
          for (AggregatorMap<EVENT> dimesion : other.aggregatorMaps) {
            if (dimesion != null) {
              newArray[i++] = dimesion;
            }
          }
          other.aggregatorMaps = newArray;


          if (this.aggregatorMaps == null) {
            @SuppressWarnings("unchecked")
            AggregatorMap<EVENT>[] newInstance = (AggregatorMap<EVENT>[])Array.newInstance(AggregatorMap.class, 1);
            this.aggregatorMaps = newInstance;
          }
          else {
            this.aggregatorMaps = Arrays.copyOf(this.aggregatorMaps, this.aggregatorMaps.length + 1);
          }

          this.aggregatorMaps[this.aggregatorMaps.length - 1] = otherMap;
          break;
        }
      }
    }
  }

  public static class PartitionerImpl<EVENT> implements Partitioner<DimensionsComputation<EVENT>>
  {
    @Override
    public void partitioned(Map<Integer, Partition<DimensionsComputation<EVENT>>> partitions)
    {
    }

    @Override
    public Collection<Partition<DimensionsComputation<EVENT>>> definePartitions(Collection<Partition<DimensionsComputation<EVENT>>> partitions, int incrementalCapacity)
    {
      if (incrementalCapacity == 0) {
        return partitions;
      }

      int newPartitionsCount = partitions.size() + incrementalCapacity;

      LinkedHashMap<Aggregator<EVENT>, DimensionsComputation<EVENT>> map = new LinkedHashMap<Aggregator<EVENT>, DimensionsComputation<EVENT>>(newPartitionsCount);
      for (Partition<DimensionsComputation<EVENT>> partition : partitions) {
        for (Aggregator<EVENT> dimension : partition.getPartitionedInstance().getAggregators()) {
          map.put(dimension, partition.getPartitionedInstance());
        }
      }

      int remainingDimensions = map.size();
      if (newPartitionsCount > remainingDimensions) {
        newPartitionsCount = remainingDimensions;
      }

      int dimensionsPerPartition[] = new int[newPartitionsCount];
      while (remainingDimensions > 0) {
        for (int i = 0; i < newPartitionsCount && remainingDimensions > 0; i++) {
          dimensionsPerPartition[i]++;
          remainingDimensions--;
        }
      }

      ArrayList<Partition<DimensionsComputation<EVENT>>> returnValue = new ArrayList<Partition<DimensionsComputation<EVENT>>>(newPartitionsCount);

      Iterator<Entry<Aggregator<EVENT>, DimensionsComputation<EVENT>>> iterator = map.entrySet().iterator();
      for (int i = 0; i < newPartitionsCount; i++) {
        DimensionsComputation<EVENT> dc = new DimensionsComputation<EVENT>();
        for (int j = 0; j < dimensionsPerPartition[i]; j++) {
          Entry<Aggregator<EVENT>, DimensionsComputation<EVENT>> next = iterator.next();
          dc.transferDimension(next.getKey(), next.getValue());
        }

        DefaultPartition<DimensionsComputation<EVENT>> partition = new DefaultPartition<DimensionsComputation<EVENT>>(dc);
        returnValue.add(partition);
      }

      return returnValue;
    }

  }

  /**
   * Kryo has an issue in prior to version 2.23 where it does not honor
   * KryoSerializer implementation.
   *
   * So we provide serializer in a different way. This code will not be
   * needed after 2.23 is released.
   *
   * It seems that a few things are still broken in 2.23.0
   * hint: map vs externalizable interface which needs to be evaluated.
   *
   * @param <T>
   */
  public static class ExternalizableSerializer<T extends Externalizable> extends Serializer<T>
  {
    @Override
    public void write(Kryo kryo, Output output, T object)
    {
      try {
        ObjectOutputStream stream;
        object.writeExternal(stream = new ObjectOutputStream(output));
        stream.flush();
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> type)
    {
      T object = kryo.newInstance(type);
      kryo.reference(object);
      try {
        object.readExternal(new ObjectInputStream(input));
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      return object;
    }

  }

  @DefaultSerializer(ExternalizableSerializer.class)
  static class AggregatorMap<EVENT> extends TCustomHashMap<EVENT, EVENT>
  {
    transient Aggregator<EVENT> aggregator;

    @SuppressWarnings("PublicConstructorInNonPublicClass")
    public AggregatorMap()
    {
      /* Needed for Serialization */
      super();
      aggregator = null;
    }

    AggregatorMap(Aggregator<EVENT> aggregator)
    {
      super(aggregator);
      this.aggregator = aggregator;
    }

    AggregatorMap(Aggregator<EVENT> aggregator, int initialCapacity)
    {
      super(aggregator, initialCapacity);
      this.aggregator = aggregator;
    }

    public void add(EVENT tuple)
    {
      EVENT event = get(tuple);
      if (event == null) {
        event = aggregator.getGroup(tuple);
        put(event, event);
      }

      aggregator.aggregate(event, tuple);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
      super.readExternal(in);
      aggregator = (Aggregator<EVENT>)super.strategy;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AggregatorMap)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      AggregatorMap<?> that = (AggregatorMap<?>)o;

      if (aggregator != null ? !aggregator.equals(that.aggregator) : that.aggregator != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = super.hashCode();
      result = 31 * result + (aggregator != null ? aggregator.hashCode() : 0);
      return result;
    }

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(AggregatorMap.class);
    private static final long serialVersionUID = 201311171410L;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DimensionsComputation)) {
      return false;
    }

    DimensionsComputation<?> that = (DimensionsComputation<?>)o;

    return Arrays.equals(aggregatorMaps, that.aggregatorMaps);

  }

  @Override
  public int hashCode()
  {
    return aggregatorMaps != null ? Arrays.hashCode(aggregatorMaps) : 0;
  }

}

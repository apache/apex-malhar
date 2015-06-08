/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.dimensions;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.AggregateMap;
import com.datatorrent.lib.dimensions.DimensionsComputation.UnifiableAggregate;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.validation.constraints.Min;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This is a simple partitioner for dimensions computation operators that extend {@link AbstractDimensionsComputation}.
 * @param <AGGREGATOR_INPUT> The type of the input data that is received by the dimensions computation operator.
 * @param <AGGREGATE> The type of the aggregated data that is output by the dimensions computation operator.
 * @param <DIMENSIONS_OPERATOR> The type of the dimensions computation operator.
 */
public class DimensionsComputationRoundRobinPartitioner<AGGREGATOR_INPUT,
                                                        AGGREGATE extends UnifiableAggregate,
                                                        DIMENSIONS_OPERATOR extends AbstractDimensionsComputation<AGGREGATOR_INPUT, AGGREGATE>>
                                                        implements Partitioner<DIMENSIONS_OPERATOR>
{
  /**
   * The number of partitions.
   */
  @Min(1)
  private int partitionCount;

  /**
   * Sets the desired number of partitions.
   * @param partitionCount The desired number of partitions.
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * Gets the desired number of partitions.
   * @return The desired number of partitions.
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  @Override
  public Collection<Partition<DIMENSIONS_OPERATOR>>
  definePartitions(Collection<Partition<DIMENSIONS_OPERATOR>> partitions,
                   PartitioningContext context)
  {
    int newPartitionsCount = DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);

    if(partitions.size() == newPartitionsCount) {
      //number of partitions is the same, no additional work required.
      return partitions;
    }

    Partition<DIMENSIONS_OPERATOR> firstPartition = partitions.iterator().next();
    DIMENSIONS_OPERATOR operator = firstPartition.getPartitionedInstance();

    Kryo kryo = new Kryo();

    if(operator.maps == null)
    {
      //This will only happen the first time define partitions is called and there will only be one operator
      Collection<Partition<DIMENSIONS_OPERATOR>> collection =
      Lists.newArrayList();

      //Allocate the desired number of partitions
      for(int index = 0;
          index < newPartitionsCount;
          index++) {
        DefaultPartition<DIMENSIONS_OPERATOR> defaultPartition =
        new DefaultPartition<DIMENSIONS_OPERATOR>(kryo.copy(operator));
        collection.add((Partition<DIMENSIONS_OPERATOR>) defaultPartition);
      }

      return collection;
    }

    if(newPartitionsCount > partitions.size()) {
      //more new partitions than old.
      Collection<Partition<DIMENSIONS_OPERATOR>> collection =
      Lists.newArrayList();

      collection.addAll(partitions);
      int remaining = partitions.size() - newPartitionsCount;

      //create a clone of the operator to intialize the new partitions.
      DIMENSIONS_OPERATOR noMap;

      try {
        noMap = clone(kryo, operator);
      }
      catch(IOException ex) {
        throw new RuntimeException(ex);
      }

      //new partitions should have a cleared map.
      noMap.maps = null;

      //initialize the new collection of partitions.
      collection.add(new DefaultPartition<DIMENSIONS_OPERATOR>(noMap));

      for(int index = 1; index < remaining; index++) {
        DIMENSIONS_OPERATOR tempOperator;

        try {
          tempOperator = clone(kryo, noMap);
        }
        catch(IOException ex) {
          throw new RuntimeException(ex);
        }

        collection.add(new DefaultPartition<DIMENSIONS_OPERATOR>(tempOperator));
      }

      return collection;
    }
    else {
      //there are fewer new partitions than old partitions
      List<Partition<DIMENSIONS_OPERATOR>> collection = Lists.newArrayList();

      //Pre allocate list
      for(int index = 0;
          index < newPartitionsCount;
          index++) {
        collection.add(null);
      }

      Iterator<Partition<DIMENSIONS_OPERATOR>> partitionIterator =
      partitions.iterator();

      //loop through the old partitions.
      for(int index = 0;
          index < partitions.size();
          index++) {
        int newIndex = index % newPartitionsCount;
        Partition<DIMENSIONS_OPERATOR> partition = partitionIterator.next();

        if(collection.get(newIndex) == null) {
          //if the new operator hasn't been set yet, set an old one.
          collection.set(newIndex, partition);
        }
        else {
          //if the new operator is set, then take the data from the old operator
          //and aggregate it with the data of the new operator.
          Partition<DIMENSIONS_OPERATOR> existing = collection.get(newIndex);

          for(int mapIndex = 0;
              mapIndex < existing.getPartitionedInstance().maps.length;
              mapIndex++) {
            AggregateMap<AGGREGATOR_INPUT, AGGREGATE> existingMap = existing.getPartitionedInstance().maps[mapIndex];
            AggregateMap<AGGREGATOR_INPUT, AGGREGATE> newMap = partition.getPartitionedInstance().maps[mapIndex];

            for(Entry<AGGREGATOR_INPUT, AGGREGATE> entry: newMap.entrySet()) {
              AGGREGATOR_INPUT aggregatorInput = entry.getKey();
              AGGREGATE newValue = entry.getValue();

              AGGREGATE oldValue = existingMap.get(aggregatorInput);

              if(oldValue == null) {
                existingMap.put(aggregatorInput, newValue);
              }
              else {
                existingMap.getAggregator().aggregate(oldValue, newValue);
              }
            }
          }
        }
      }

      //return the new operators
      return (Collection<Partition<DIMENSIONS_OPERATOR>>) collection;
    }
  }

  /**
   * This is a utility method to clone operators.
   * @param <T> The type of the operator to clone.
   * @param kryo The kryo object to use for cloning.
   * @param src The source operator to clone.
   * @return The cloned operator.
   * @throws IOException
   */
  public static <T> T clone(Kryo kryo, T src) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    kryo.writeObject(output, src);
    output.close();
    Input input = new Input(bos.toByteArray());
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)src.getClass();
    return kryo.readObject(input, clazz);
  }

  @Override
  public void partitioned(Map<Integer, Partition<DIMENSIONS_OPERATOR>> partitions)
  {
    //Do nothing
  }
}

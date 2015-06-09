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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.dimensions.DimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import gnu.trove.map.hash.TCustomHashMap;
import gnu.trove.strategy.HashingStrategy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This is a base class for dimensions computation. This operator performs dimension computation by
 * utilizing {@link AggregatorMap}s. The {@link AggregatorMap} is intended to simplify aggregating
 * data. Each {@link AggregatorMap} has three things assigned to it: an
 * aggregate index, an {@link Aggregator}, and a {@link DimensionsCombination}. The aggregate index is
 * used by the unifier to unify the correct aggregates together. The {@link Aggregator} is used by the
 * {@link AggregatorMap} to perform the correct aggregations on the data it receives. The {@link DimensionsCombination}
 * is used to determine the key combination / dimensions descriptor that the {@link AggregatorMap} is responsible
 * for aggregating over.
 * </p>
 * @param <AGGREGATOR_INPUT>
 * @param <AGGREGATE>
 */
public abstract class AbstractDimensionsComputation<AGGREGATOR_INPUT, AGGREGATE extends UnifiableAggregate> implements Operator
{
  /**
   * The aggregate maps in which aggregates are created.
   */
  @VisibleForTesting
  public AggregateMap<AGGREGATOR_INPUT, AGGREGATE>[] maps;
  /**
   * The unifier for this operator.
   */
  protected DimensionsComputationUnifier<AGGREGATOR_INPUT, AGGREGATE> unifier;
  /**
   * The hashing strategy to set on the unifier.
   */
  protected DTHashingStrategy<AGGREGATE> unifierHashingStrategy;

  public final transient DefaultOutputPort<AGGREGATE> output = new DefaultOutputPort<AGGREGATE>() {
    @Override
    public Unifier<AGGREGATE> getUnifier()
    {
      configureDimensionsComputationUnifier();
      return unifier;
    }
  };

  /**
   * Constructor for creating the operator.
   */
  public AbstractDimensionsComputation()
  {
    //Do nothing
  }

  /**
   * Sets the unifier for this operator.
   * @param unifier The unifier for this operator.
   */
  public void setUnifier(DimensionsComputationUnifier<AGGREGATOR_INPUT, AGGREGATE> unifier)
  {
    this.unifier = unifier;
  }

  /**
   * This method configures the unifier set on this operator with the appropriate aggregators.
   * @return The aggregators set on the unifier.
   */
  @VisibleForTesting
  public abstract Aggregator<AGGREGATOR_INPUT, AGGREGATE>[] configureDimensionsComputationUnifier();

  @Override
  @SuppressWarnings("unchecked")
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    //Emit the contents of the aggregate map
    for(AggregateMap<AGGREGATOR_INPUT, AGGREGATE> map: maps) {
      for(AGGREGATE value: map.values()) {
        output.emit(value);
      }

      map.clear();
    }
  }

  @Override
  public void teardown()
  {
  }

  /**
   * This class is used to serialize an {@link AggregateMap}.
   * @param <AGGREGATOR_INPUT> The type of the input data received by the dimensions computation operator.
   * @param <AGGREGATE> The type of the aggregate data emitted by the dimensions computation operator.
   */
  public static class ExternalizableSerializer<AGGREGATOR_INPUT, AGGREGATE extends UnifiableAggregate> extends Serializer<AggregateMap<AGGREGATOR_INPUT, AGGREGATE>>
  {
    @Override
    public void write(Kryo kryo, Output output, AggregateMap<AGGREGATOR_INPUT, AGGREGATE> object)
    {
      try {
        ObjectOutputStream stream = new ObjectOutputStream(output);

        //serialize the map first, because TCustomHashmap clears output stream
        object.writeExternal(stream);
        //serialize the aggregator for this map
        writeObject(object.aggregator, stream);
        //serialize the hashing strategy for this map
        writeObject(object.hashingStrategy, stream);

        stream.flush();
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    /**
     * This is a helper method which writes the given object to the given {@link ObjectOutputStream}. This is done
     * with the following steps:
     * <ol>
     *  <li>Serializing the object using java serialization.</li>
     *  <li>Writing the byte length of the serialized object out to the given {@link ObjectOutputStream}.</li>
     *  <li>Writing the serialized object out to the given {@link ObjectOutputStream}.</li>
     * </ol>
     * @param object The object to write out to the given output stream.
     * @param stream The stream to which the given object is written out to.
     */
    private void writeObject(Object object, ObjectOutputStream stream)
    {
      ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream fieldStream;

      try {
        //Java serialization
        fieldStream = new ObjectOutputStream(byteOutputStream);
        fieldStream.writeObject(object);
        fieldStream.flush();
        fieldStream.close();
        //Object bytes
        byte[] fieldBytes = byteOutputStream.toByteArray();
        //write out size of object bytes
        stream.writeInt(fieldBytes.length);
        //write out serialized object
        stream.write(fieldBytes);
      }
      catch(IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public AggregateMap<AGGREGATOR_INPUT, AGGREGATE> read(Kryo kryo, Input input, Class<AggregateMap<AGGREGATOR_INPUT, AGGREGATE>> type)
    {
      AggregateMap<AGGREGATOR_INPUT, AGGREGATE> object = kryo.newInstance(type);

      try {
        kryo.reference(object);

        ObjectInputStream objectInputStream = new ObjectInputStream(input);
        //read in the map
        object.readExternal(objectInputStream);
        //read in the aggregator
        object.aggregator = readObject(objectInputStream);
        //read in the hashing strategy
        object.hashingStrategy = readObject(objectInputStream);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      catch(ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }

      return object;
    }

    /**
     * This is a helper method, which reads an object from the given {@link ObjectInputStream}. It is assumed
     * that the object read from the input stream was written with the {@link #writeObject} method.
     * @param <T> The type of the object read from the input stream.
     * @param ois The {@link ObjectInputStream} to read the object from.
     * @return An object read from the given {@link ObjectInputStream}.
     */
    private static <T> T readObject(ObjectInputStream ois)
    {
      try {
        int objectBytesSize = ois.readInt();
        byte[] objectBytes = new byte[objectBytesSize];
        ois.readFully(objectBytes);

        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteBufferInputStream(objectBytes));
        @SuppressWarnings("unchecked")
        T tempObject = (T)objectInputStream.readObject();
        return tempObject;
      }
      catch(IOException ex) {
        throw new RuntimeException(ex);
      }
      catch(ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }
    }

    /**
     * This is an input stream that reads from a given byte array.
     */
    private static class ByteBufferInputStream extends InputStream
    {
      /**
       * The current index in the byte array.
       */
      private int index = 0;
      /**
       * The byte array to read from.
       */
      private byte[] bytes;

      /**
       * Creates a {@link ByteBufferInputStream} which reads from the given byte array.
       * @param bytes The byte array from which to read data.
       */
      public ByteBufferInputStream(byte[] bytes)
      {
        this.bytes = bytes;
      }

      @Override
      public int read() throws IOException
      {
        if(index == bytes.length) {
          return -1;
        }

        return bytes[index++];
      }
    }
  }

  /**
   * This class is responsible for storing aggregates, and aggregating input with the correct aggregates.
   * @param <AGGREGATOR_INPUT> The type of input values to be aggregated.
   * @param <AGGREGATE> The type of aggregate.
   */
  @DefaultSerializer(ExternalizableSerializer.class)
  public static class AggregateMap<AGGREGATOR_INPUT, AGGREGATE extends UnifiableAggregate> extends TCustomHashMap<AGGREGATOR_INPUT, AGGREGATE>
  {
    private static final long serialVersionUID = 201505200427L;
    /**
     * The aggregator to use for this aggregator map
     */
    private Aggregator<AGGREGATOR_INPUT, AGGREGATE> aggregator;
    /**
     * The dimensions combination used for this aggregate map
     */
    private DimensionsCombination<AGGREGATOR_INPUT, AGGREGATE> hashingStrategy;
    /**
     * The aggregateIndex assigned to this aggregate map.
     */
    private int aggregateIndex;

    /**
     * Constructor to create map.
     */
    public AggregateMap()
    {
      // Needed for Serialization
      super();
    }

    /**
     * Creates the aggregate map with the given aggregator and dimensions combination.
     * @param aggregator The aggregator to use for this aggregate map.
     * @param hashingStrategy The dimensions combination to use for this aggregate map.
     * @param aggregateIndex The aggregate index assigned to this aggregate map.
     */
    public AggregateMap(Aggregator<AGGREGATOR_INPUT, AGGREGATE> aggregator,
                        DimensionsCombination<AGGREGATOR_INPUT, AGGREGATE> hashingStrategy,
                        int aggregateIndex)
    {
      super(hashingStrategy);

      this.hashingStrategy = hashingStrategy;
      this.aggregator = Preconditions.checkNotNull(aggregator);
      this.aggregateIndex = aggregateIndex;
    }

    /**
     * Sets the aggregator for this aggregate map.
     * @param aggregator The aggregator to use for this aggregate map.
     */
    public void setAggregator(Aggregator<AGGREGATOR_INPUT, AGGREGATE> aggregator)
    {
      this.aggregator = Preconditions.checkNotNull(aggregator);
    }

    /**
     * Returns the aggregator for the aggregate map.
     * @return The aggregator for the aggregate map.
     */
    public Aggregator<AGGREGATOR_INPUT, AGGREGATE> getAggregator()
    {
      return aggregator;
    }

    /**
     * Aggregates the given input with the correct aggregate if the corresponding aggregate already
     * exists. If the corresponding aggregate does not already exist, then it is created from the
     * aggregatorInput.
     * @param aggregatorInput The input event to aggregate to an aggregation.
     */
    public void aggregate(AGGREGATOR_INPUT aggregatorInput)
    {
      AGGREGATE aggregate = get(aggregatorInput);

      if(aggregate == null) {
        aggregate = aggregator.createDest(aggregatorInput);
        aggregate.setAggregateIndex(aggregateIndex);
        hashingStrategy.setKeys(aggregatorInput, aggregate);
        put(aggregatorInput, aggregate);
      }
      else {
        aggregator.aggregate(aggregate, aggregatorInput);
      }
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 23 * hash + (this.aggregator != null ? this.aggregator.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if(obj == null) {
        return false;
      }
      if(getClass() != obj.getClass()) {
        return false;
      }
      final AggregateMap<?, ?> other = (AggregateMap<?, ?>)obj;
      if(this.aggregator != other.aggregator && (this.aggregator == null || !this.aggregator.equals(other.aggregator))) {
        return false;
      }
      return true;
    }
  }

  /**
   * This interface is used to mask the {@link HashingStrategy} interface, which will likely be removed in the future.
   * @param <AGGREGATOR_INPUT> The type of input values to be aggregated.
   */
  public static interface DTHashingStrategy<AGGREGATOR_INPUT> extends HashingStrategy<AGGREGATOR_INPUT> {}

  /**
   * <p>
   * A {@link DimensionsCombination} is a class that defines a dimensions combination for dimensions computation operator.
   * A dimension combination is defined by implementing a hashing strategy which maps inputs to aggregates with the same key combination.
   * Additionally a dimension combination is used to initialize the keys of a newly create aggregate with the {@link setKeys}
   * method.
   * </p>
   * <p>
   * An example of a DimensionsCombination is the following:
   * <br/>
   * <br/>
   * Consider the following class:
   * <br/>
   * <br/>
   * {@code
   *  public class AdInfo
   *  {
   *    @NotNull
   *    public String advertiser;
   *    @NotNull
   *    public String publisher;
   *    @NotNull
   *    public String location;
   *  }
   *
   *  public class AdInfoAggregate
   *  {
   *    public String advertiser;
   *    public String publisher;
   *    public String location;
   *    ...
   *  }
   * }
   * <br/>
   * <br/>
   * An {@link AggregatorMap} responsible for aggregating across different combinations of advertisers and publishers
   * would have a {@link DimensionsCombination} which looks like the following:
   * {@code
   *  public class PublisherAndAdvertiserCombination<AdInfo, AdInfoAggregate> implements DimensionsCombination<AdInfo, AdInfoAggregate>
   *  {
   *    public int computeHashCode(AdInfo adInfo)
   *    {
   *      return adInfo.advertiser.hashCode() ^ adInfo.publisher.hashCode();
   *    }
   *
   *    public equals(AdInfo a, AdInfo b)
   *    {
   *      return a.advertiser.equals(b.advertiser) && a.publisher.equals(b.publisher);
   *    }
   *
   *    public void setKeys(AdInfo aggregatorInput, AdInfoAggregate aggregate)
   *    {
   *      aggregate.publisher = aggregatorInput.publisher;
   *      aggregate.advertiser = aggregatorInput.advertiser;
   *    }
   *  }
   * }
   * </p>
   * <br/>
   * As can be seen above, the {@link #computeHashCode} and equals method only utilize publisher and advertiser. So the {@link AggregatorMap}
   * only aggregates values with the
   * @param <AGGREGATOR_INPUT> The type of input values to be aggregated.
   * @param <AGGREGATE> The type of aggregate.
   */
  public static interface DimensionsCombination<AGGREGATOR_INPUT, AGGREGATE> extends DTHashingStrategy<AGGREGATOR_INPUT>
  {
    /**
     * This method sets the keys derived from the given aggregator input onto the given aggregate.
     * @param aggregatorInput The aggregator input from which to derive the keys which comprise this dimension combination.
     * @param aggregate The aggregate on which to set the dimensions combination keys.
     */
    public void setKeys(AGGREGATOR_INPUT aggregatorInput, AGGREGATE aggregate);
  }

  /**
   * This is a hashing strategy that utilizes the {@link java.lang.Object#equals} and {@link java.lang.Object#hashCode} methods.
   * @param <AGGREGATOR_INPUT> The type of input values to be aggregated.
   * @param <AGGREGATE> The type of aggregate.
   */
  public static class DirectDimensionsCombination<AGGREGATOR_INPUT, AGGREGATE> implements DimensionsCombination<AGGREGATOR_INPUT, AGGREGATE>
  {
    private static final long serialVersionUID = 201505230252L;

    /**
     * Constructor to create object.
     */
    public DirectDimensionsCombination()
    {
      //Do nothing
    }

    @Override
    public int computeHashCode(AGGREGATOR_INPUT o)
    {
      return o.hashCode();
    }

    @Override
    public boolean equals(AGGREGATOR_INPUT a, AGGREGATOR_INPUT b)
    {
      return a.equals(b);
    }

    @Override
    public void setKeys(AGGREGATOR_INPUT aggregatorInput, AGGREGATE aggregate)
    {
      //NOOP
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDimensionsComputation.class);
}

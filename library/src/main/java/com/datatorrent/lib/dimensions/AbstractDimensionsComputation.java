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
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.UnifiableAggregate;
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

public abstract class AbstractDimensionsComputation<AGGREGATOR_INPUT, AGGREGATE extends UnifiableAggregate> implements Operator
{
  @VisibleForTesting
  public AggregateMap<AGGREGATOR_INPUT, AGGREGATE>[] maps;
  protected DimensionsComputationUnifier<AGGREGATOR_INPUT, AGGREGATE> unifier;
  protected DTHashingStrategy<AGGREGATE> unifierHashingStrategy;

  public final transient DefaultOutputPort<AGGREGATE> output = new DefaultOutputPort<AGGREGATE>() {
    @Override
    public Unifier<AGGREGATE> getUnifier()
    {
      configureDimensionsComputationUnifier();
      return unifier;
    }
  };

  public AbstractDimensionsComputation()
  {
  }

  public void setUnifier(DimensionsComputationUnifier<AGGREGATOR_INPUT, AGGREGATE> unifier)
  {
    this.unifier = unifier;
  }

  public abstract void configureDimensionsComputationUnifier();

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

  public static class ExternalizableSerializer<AGGREGATOR_INPUT, AGGREGATE extends UnifiableAggregate> extends Serializer<AggregateMap<AGGREGATOR_INPUT, AGGREGATE>>
  {
    @Override
    public void write(Kryo kryo, Output output, AggregateMap<AGGREGATOR_INPUT, AGGREGATE> object)
    {
      try {
        ObjectOutputStream stream = new ObjectOutputStream(output);

        object.writeExternal(stream);
        writeObject(object.aggregator, stream);
        writeObject(object.hashingStrategy, stream);

        stream.flush();
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    private void writeObject(Object object, ObjectOutputStream stream)
    {
      ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream fieldStream;

      try {
        fieldStream = new ObjectOutputStream(byteOutputStream);
        fieldStream.writeObject(object);
        fieldStream.flush();
        fieldStream.close();
        byte[] fieldBytes = byteOutputStream.toByteArray();
        stream.writeInt(fieldBytes.length);
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
        object.readExternal(objectInputStream);

        object.aggregator = readObject(objectInputStream);
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

    private static class ByteBufferInputStream extends InputStream
    {
      private int index = 0;
      private byte[] bytes;

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

  @DefaultSerializer(ExternalizableSerializer.class)
  public static class AggregateMap<AGGREGATOR_INPUT, AGGREGATE extends UnifiableAggregate> extends TCustomHashMap<AGGREGATOR_INPUT, AGGREGATE>
  {
    private static final long serialVersionUID = 201505200427L;
    private transient Aggregator<AGGREGATOR_INPUT, AGGREGATE> aggregator;
    private int aggregateIndex;
    private DimensionsCombination<AGGREGATOR_INPUT, AGGREGATE> hashingStrategy;

    public AggregateMap()
    {
      /* Needed for Serialization */
      super();
      LOG.debug("No arg constructor");
    }

    public AggregateMap(Aggregator<AGGREGATOR_INPUT, AGGREGATE> aggregator,
                        DimensionsCombination<AGGREGATOR_INPUT, AGGREGATE> hashingStrategy,
                        int aggregateIndex)
    {
      super(hashingStrategy);

      this.hashingStrategy = hashingStrategy;
      this.aggregator = Preconditions.checkNotNull(aggregator);
      this.aggregateIndex = aggregateIndex;
    }

    public void setAggregator(Aggregator<AGGREGATOR_INPUT, AGGREGATE> aggregator)
    {
      this.aggregator = aggregator;
    }

    public Aggregator<AGGREGATOR_INPUT, AGGREGATE> getAggregator()
    {
      return aggregator;
    }

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

  public static interface DTHashingStrategy<AGGREGATOR_INPUT> extends HashingStrategy<AGGREGATOR_INPUT> {}

  public static interface DimensionsCombination<AGGREGATOR_INPUT, AGGREGATE> extends DTHashingStrategy<AGGREGATOR_INPUT>
  {
    public void setKeys(AGGREGATOR_INPUT aggregatorInput, AGGREGATE aggregate);
  }

  public static class DirectDimensionsCombination<AGGREGATOR_INPUT, AGGREGATE> implements DimensionsCombination<AGGREGATOR_INPUT, AGGREGATE>
  {
    private static final long serialVersionUID = 201505230252L;

    public DirectDimensionsCombination()
    {
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

  public interface AggregateResult {}

  public interface UnifiableAggregate extends AggregateResult
  {
    public int getAggregateIndex();
    public void setAggregateIndex(int aggregateIndex);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDimensionsComputation.class);
}

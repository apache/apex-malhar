/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.counters;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.apache.commons.lang.mutable.Mutable;

import com.datatorrent.api.Context;

import com.datatorrent.common.util.NumberAggregate;

/**
 * Implementation of basic number counters.
 *
 * @param <T> type of counter
 * @since 1.0.2
 */
@JsonSerialize(using = BasicCounters.Serializer.class)
public class BasicCounters<T extends Number & Mutable> implements Serializable
{
  private final Map<Enum<?>, T> cache;
  private final Class<T> counterType;

  @SuppressWarnings("unused")
  private BasicCounters()
  {
    //for kryo
    cache = null;
    counterType = null;
  }

  /**
   * @param counterType type of counter
   */
  public BasicCounters(@Nonnull Class<T> counterType)
  {
    cache = Maps.newIdentityHashMap();
    this.counterType = counterType;
  }

  /**
   * Returns the counter associated with the key. Creates it if it doesn't exist.
   */
  @SuppressWarnings("unchecked")
  public synchronized T findCounter(Enum<?> counterKey) throws IllegalAccessException, InstantiationException
  {
    T counter = cache.get(counterKey);
    if (counter == null) {
      counter = counterType.newInstance();
      cache.put(counterKey, counter);
    }
    return counter;
  }

  /**
   * Returns the counter if it exists; null otherwise
   *
   * @param counterKey
   * @return counter corresponding to the counter key.
   */
  public synchronized T getCounter(Enum<?> counterKey)
  {
    return cache.get(counterKey);
  }

  /**
   * Sets the value of a counter.
   *
   * @param counterKey counter key.
   * @param value      new value.
   */
  public synchronized void setCounter(Enum<?> counterKey, T value)
  {
    cache.put(counterKey, value);
  }

  /**
   * Returns an immutable copy of all the counters.
   *
   * @return an immutable copy of the counters.
   */
  public synchronized ImmutableMap<Enum<?>, T> getCopy()
  {
    return ImmutableMap.copyOf(cache);
  }

  public static class Serializer extends JsonSerializer<BasicCounters<?>> implements Serializable
  {

    @Override
    public void serialize(BasicCounters<?> value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException
    {
      jgen.writeObject(value.getCopy());
    }

    private static final long serialVersionUID = 201406230131L;

  }

  /**
   * Aggregates the {@link BasicCounters} of physical partitions into {@link NumberAggregate.LongAggregate}.
   *
   * @param <T> type of counter
   */
  public static class LongAggregator<T extends Number & Mutable> implements Context.CountersAggregator, Serializable
  {
    @Override
    public Object aggregate(Collection<?> objects)
    {
      Map<String, NumberAggregate.LongAggregate> aggregates = Maps.newHashMap();
      for (Object counter : objects) {
        if (counter instanceof BasicCounters) {
          @SuppressWarnings("unchecked")
          BasicCounters<T> physical = (BasicCounters<T>) counter;
          ImmutableMap<Enum<?>, T> copy = physical.getCopy();

          for (Map.Entry<Enum<?>, T> entry : copy.entrySet()) {

            NumberAggregate.LongAggregate aggregate = aggregates.get(entry.getKey().name());

            if (aggregate == null) {
              aggregate = new NumberAggregate.LongAggregate();

              aggregates.put(entry.getKey().name(), aggregate);
            }
            aggregate.addNumber(entry.getValue().longValue());
          }
        }
      }
      return ImmutableMap.copyOf(aggregates);
    }

    private static final long serialVersionUID = 201406222203L;
  }

  /**
   * Aggregates the {@link BasicCounters} of physical partitions into {@link NumberAggregate.DoubleAggregate}.
   *
   * @param <T> type of counter
   */
  public static class DoubleAggregator<T extends Number & Mutable> implements Context.CountersAggregator, Serializable
  {
    @Override
    public Object aggregate(Collection<?> objects)
    {
      Map<String, NumberAggregate.DoubleAggregate> aggregates = Maps.newHashMap();
      for (Object counter : objects) {
        if (counter instanceof BasicCounters) {
          @SuppressWarnings("unchecked")
          BasicCounters<T> physical = (BasicCounters<T>) counter;
          ImmutableMap<Enum<?>, T> copy = physical.getCopy();

          for (Map.Entry<Enum<?>, T> entry : copy.entrySet()) {

            NumberAggregate.DoubleAggregate aggregate = aggregates.get(entry.getKey().name());

            if (aggregate == null) {
              aggregate = new NumberAggregate.DoubleAggregate();

              aggregates.put(entry.getKey().name(), aggregate);
            }
            aggregate.addNumber(entry.getValue().doubleValue());
          }
        }
      }
      return ImmutableMap.copyOf(aggregates);
    }

    private static final long serialVersionUID = 201407011713L;
  }

  private static final long serialVersionUID = 201406230033L;
}

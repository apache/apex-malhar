package org.apache.apex.malhar.lib.state.spillable;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.ManagedStateContext;
import org.apache.apex.malhar.lib.state.managed.TimeBucketAssigner;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by david on 8/31/16.
 */
public class SpillableTimeKeyedMapImpl<K, V>
    extends SpillableByteMapImpl<K, V>
    implements Spillable.SpillableIterableByteMap<K, V>, Spillable.SpillableComponent, Serializable
{
  @NotNull
  private SpillableTimeStateStore store;

  @NotNull
  private Function<K, Long> timestampExtractor;
  private final long millisPerTimeBucket;

  /**
   * Creates a {@link SpillableTimeKeyedMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeKeyedMapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableTimeKeyedMapImpl} in the provided {@link SpillableTimeStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableTimeKeyedMapImpl(SpillableTimeStateStore store, byte[] identifier, long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue, Function<K, Long> timestampExtractor, long millisPerTimeBucket)
  {
    super(store, identifier, bucket, serdeKey, serdeValue);
    this.store = Preconditions.checkNotNull(store);
    this.timestampExtractor = timestampExtractor;
    this.millisPerTimeBucket = millisPerTimeBucket;
  }

  private long extractTimeFromKey(K key)
  {
    return timestampExtractor.apply(key);
  }


  @Override
  public SpillableTimeStateStore getStore()
  {
    return this.store;
  }

  @Override
  protected Slice getSliceFromStore(K key)
  {
    long time = extractTimeFromKey(key);
    return store.getSync(bucket, time, SliceUtils.concatenate(identifier, serdeKey.serialize(key)));
  }

  @Override
  protected void putSliceToStore(K key, Slice valueSlice)
  {
    long time = extractTimeFromKey(key);
    store.put(this.bucket, time, SliceUtils.concatenate(identifier, serdeKey.serialize(key)), valueSlice);
  }

  /**
   * This returns an iterator that iterates through entries with keys greater than or equal to the given key
   * that are in the same time bucket.
   * TODO: We might want to change this in the future to support returning keys in ALL time buckets
   *
   * @param key
   * @return
   */
  @Override
  public PeekingIterator<Map.Entry<K, V>> iterator(final K key)
  {
    return new PeekingIterator<Map.Entry<K, V>>()
    {
      private PeekingIterator<Map.Entry<Slice, Slice>> internalIterator = store.iterator(bucket, extractTimeFromKey(key), SliceUtils.concatenate(identifier, serdeKey.serialize(key)));
      private K lastKey;

      @Override
      public boolean hasNext()
      {
        while (internalIterator.hasNext()) {
          Map.Entry<Slice, Slice> nextEntry = internalIterator.peek();
          K key = serdeKey.deserialize(nextEntry.getKey());
          V value = get(key);
          if (value == null) {
            internalIterator.next();
          } else {
            return true;
          }
        }
        return false;
      }

      @Override
      public Map.Entry<K, V> next()
      {
        while (internalIterator.hasNext()) {
          Map.Entry<Slice, Slice> nextEntry = internalIterator.next();
          K key = serdeKey.deserialize(nextEntry.getKey());
          V value = get(key);
          if (value != null) {
            lastKey = key;
            return new AbstractMap.SimpleEntry<>(serdeKey.deserialize(nextEntry.getKey()), serdeValue
                .deserialize(nextEntry.getValue()));
          }
        }
        throw new NoSuchElementException();
      }

      @Override
      public void remove()
      {
        Preconditions.checkNotNull(lastKey);
        SpillableTimeKeyedMapImpl.this.remove(lastKey);
      }

      @Override
      public Map.Entry<K, V> peek()
      {
        while (internalIterator.hasNext()) {
          Map.Entry<Slice, Slice> nextEntry = internalIterator.peek();
          K key = serdeKey.deserialize(nextEntry.getKey());
          V value = get(key);
          if (value == null) {
            internalIterator.next();
          } else {
            return new AbstractMap.SimpleEntry<>(key, value);
          }
        }
        throw new NoSuchElementException();
      }
    };
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.store.setTimeBucketAssigner(new MyTimeBucketAssigner());
    super.setup(context);
  }



  private class MyTimeBucketAssigner extends TimeBucketAssigner
  {
    // TODO: purging based on lateness horizon

    @Override
    public long getTimeBucketAndAdjustBoundaries(long value)
    {
      return value / millisPerTimeBucket;
    }

    @Override
    public void setup(@NotNull ManagedStateContext managedStateContext)
    {
    }

    @Override
    public void endWindow()
    {
    }
  }
}

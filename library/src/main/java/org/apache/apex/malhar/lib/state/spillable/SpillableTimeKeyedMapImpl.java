package org.apache.apex.malhar.lib.state.spillable;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.managed.ManagedStateContext;
import org.apache.apex.malhar.lib.state.managed.TimeBucketAssigner;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by david on 8/31/16.
 */
public class SpillableTimeKeyedMapImpl<K, V> implements Spillable.SpillableIterableByteMap<K, V>, Spillable.SpillableComponent,
    Serializable
{
  private transient WindowBoundedMapCache<K, V> cache = new WindowBoundedMapCache<>();
  private transient MutableInt tempOffset = new MutableInt();

  @NotNull
  private SpillableTimeStateStore store;
  @NotNull
  private byte[] identifier;
  protected long bucket;
  @NotNull
  protected Serde<K, Slice> serdeKey;
  @NotNull
  protected Serde<V, Slice> serdeValue;
  private int size = 0;

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
    this.store = Preconditions.checkNotNull(store);
    this.identifier = Preconditions.checkNotNull(identifier);
    this.bucket = bucket;
    this.serdeKey = Preconditions.checkNotNull(serdeKey);
    this.serdeValue = Preconditions.checkNotNull(serdeValue);
    this.timestampExtractor = timestampExtractor;
    this.millisPerTimeBucket = millisPerTimeBucket;
  }

  private long extractTimeFromKey(K key)
  {
    return timestampExtractor.apply(key);
  }


  public SpillableTimeStateStore getStore()
  {
    return this.store;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public boolean isEmpty()
  {
    return size == 0;
  }

  @Override
  public boolean containsKey(Object o)
  {
    return get(o) != null;
  }

  @Override
  public boolean containsValue(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object o)
  {
    K key = (K)o;

    if (cache.getRemovedKeys().contains(key)) {
      return null;
    }

    V val = cache.get(key);

    if (val != null) {
      return val;
    }

    long time = extractTimeFromKey(key);
    Slice valSlice = store.getSync(bucket, time, SliceUtils.concatenate(identifier, serdeKey.serialize(key)));

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    tempOffset.setValue(0);
    return serdeValue.deserialize(valSlice, tempOffset);
  }

  @Override
  public V put(K k, V v)
  {
    V value = get(k);

    if (value == null) {
      size++;
    }

    cache.put(k, v);

    return value;
  }

  @Override
  public V remove(Object o)
  {
    V value = get(o);

    if (value != null) {
      size--;
    }

    cache.remove((K)o);

    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map)
  {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
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
  public PeekingIterator<Entry<K, V>> iterator(final K key)
  {

    return new PeekingIterator<Entry<K, V>>()
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
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<K, V>> entrySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.store.setTimeBucketAssigner(new MyTimeBucketAssigner());
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {

    for (K key: cache.getChangedKeys()) {
      long time = extractTimeFromKey(key);
      store.put(this.bucket, time, SliceUtils.concatenate(identifier, serdeKey.serialize(key)),
          serdeValue.serialize(cache.get(key)));
    }

    for (K key: cache.getRemovedKeys()) {
      long time = extractTimeFromKey(key);
      store.put(this.bucket, time, SliceUtils.concatenate(identifier, serdeKey.serialize(key)),
          new Slice(ArrayUtils.EMPTY_BYTE_ARRAY));
    }

    cache.endWindow();
  }

  @Override
  public void teardown()
  {
  }

  private class MyTimeBucketAssigner extends TimeBucketAssigner
  {
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

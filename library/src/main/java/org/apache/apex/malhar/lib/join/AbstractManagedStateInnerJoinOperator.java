/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.state.managed.KeyBucketExtractor;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.SequentialSpillableIdentifierGenerator;
import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.state.spillable.SpillableIdentifierGenerator;
import org.apache.apex.malhar.lib.state.spillable.SpillableTimeSlicedArrayListMultiMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableTimeSlicedMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableTimeStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedTimeStateSpillableStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;

/**
 * An abstract implementation of inner join operator over Managed state which extends from
 * AbstractInnerJoinOperator.
 *
 * <b>Properties:</b><br>
 * <b>noOfBuckets</b>: Number of buckets required for Managed state. <br>
 * <b>bucketSpanTime</b>: Indicates the length of the time bucket. <br>
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractManagedStateInnerJoinOperator<K,T> extends AbstractInnerJoinOperator<K,T> implements
    Operator.CheckpointNotificationListener, Operator.IdleTimeHandler
{
  public static final String stateDir = "managedState";
  public static final String streamState = "streamData";
  private transient Map<JoinEvent<K,T>, Future<List>> waitingEvents = Maps.newLinkedHashMap();
  private int noOfBuckets = 1;
  private Long bucketSpanTime;
  protected ManagedTimeStateSpillableStore streamStore;
  protected transient TimeExtractor stream1TimeExtractor;
  protected transient TimeExtractor stream2TimeExtractor;

  /**
   * Create Managed states and stores for both the streams.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void createStores()
  {
    component = new SpillableTimeSlicedComplexComponentImpl(streamStore);
    SpillableTimeSlicedComplexComponentImpl timeSlicedComponent = (SpillableTimeSlicedComplexComponentImpl)component;
    if (isLeftKeyPrimary()) {
      stream1Data = timeSlicedComponent.newSpillableMap(getKeySerde(), getValueSerde(), stream1TimeExtractor, getKeyBucketExtractor(true));
    } else {
      stream1Data = timeSlicedComponent.newSpillableArrayListMultimap(getKeySerde(), getValueSerde(),stream1TimeExtractor, getKeyBucketExtractor(true));
    }

    if (isRightKeyPrimary()) {
      stream2Data = timeSlicedComponent.newSpillableMap(getKeySerde(), getValueSerde(), stream1TimeExtractor, getKeyBucketExtractor(false));
    } else {
      stream2Data = timeSlicedComponent.newSpillableArrayListMultimap(getKeySerde(), getValueSerde(), stream1TimeExtractor, getKeyBucketExtractor(false));
    }
  }

  /**
   * Process the tuple which are received from input ports with the following steps:
   * 1) Extract key from the given tuple
   * 2) Insert <key,tuple> into the store where store is the stream1Data if the tuple
   * receives from stream1 or viceversa.
   * 3) Get the values of the key in asynchronous if found it in opposite store
   * 4) If the future is done then Merge the given tuple and values found from step (3) otherwise
   *    put it in waitingEvents
   * @param tuple given tuple
   * @param isStream1Data Specifies whether the given tuple belongs to stream1 or not.
   */
  @SuppressWarnings("unchecked")
  @Override
  protected void processTuple(T tuple, boolean isStream1Data)
  {
    Spillable.SpillableListMultimap<K,T> store = isStream1Data ? stream1Data : stream2Data;
    K key = extractKey(tuple,isStream1Data);

    // Checks whether the tuple is expired or not.
    // If it is expired, don't insert into the map.
    if (isExpiredTuple(tuple,isStream1Data)) {
      return;
    }
    store.put(key, tuple);
    Future<List> future;

    if (isStream1Data) {
      future = getAsync(stream2Data, isRightKeyPrimary(), key);
    } else {
      future = getAsync(stream1Data, isLeftKeyPrimary(), key);
    }

    if (future == null) {
      return;
    }

    if (future.isDone()) {
      try {
        joinStream(tuple,isStream1Data, future.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    } else {
      waitingEvents.put(new JoinEvent<>(key,tuple,isStream1Data),future);
    }
  }

  /**
   * Check whether the given tuple is expired tuple or not
   * @param tuple given tuple
   * @param isStream1Data Specifies whether the given tuple belongs to stream1 or not.
   * @return true if the given tuple is expired.
   */
  @SuppressWarnings("unchecked")
  boolean isExpiredTuple(T tuple, boolean isStream1Data)
  {
    TimeExtractor timeExtractor = isStream1Data ? stream1TimeExtractor : stream2TimeExtractor;
    if (timeExtractor == null) {
      return false;
    }
    long time = timeExtractor.getTime(tuple);
    return (streamStore.getTimeBucketAssigner().getTimeBucketAndAdjustBoundaries(time) == -1);
  }

  /**
   * Called the getAsync() from the given map
   * @param map given map
   * @param isPrimary Specifies whether the map is multimap or not.
   * @param key given key
   * @return Future<List>
   */
  @SuppressWarnings("unchecked")
  Future<List> getAsync(Spillable.SpillableListMultimap<K,T> map, boolean isPrimary, K key)
  {
    if (isPrimary) {
      return ((SpillableMapAsMultiMapImpl)map).getAsync(key);
    }
    return ((SpillableTimeSlicedArrayListMultiMapImpl)map).getAsync(key);
  }

  @Override
  public void handleIdleTime()
  {
    if (waitingEvents.size() > 0) {
      processWaitEvents(false);
    }
  }

  @Override
  public void beforeCheckpoint(long l)
  {
    component.beforeCheckpoint(l);
  }

  @Override
  public void checkpointed(long l)
  {
    component.checkpointed(l);
  }

  @Override
  public void committed(long l)
  {
    component.committed(l);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (streamStore == null) {
      streamStore = new ManagedTimeStateSpillableStore();
      streamStore.setNumBuckets(noOfBuckets);
      if (bucketSpanTime != null) {
        streamStore.getTimeBucketAssigner().setBucketSpan(Duration.millis(bucketSpanTime));
      }
      streamStore.getTimeBucketAssigner().setExpireBefore(Duration.millis(getExpiryTime()));
    }
    ((FileAccessFSImpl)streamStore.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + stateDir + Path.SEPARATOR + String.valueOf(context.getId()) + Path.SEPARATOR + streamState);
    stream1TimeExtractor = getTimeExtractor(true);
    stream2TimeExtractor = getTimeExtractor(false);
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  /**
   * Process the waiting events
   * @param finalize finalize Whether or not to wait for future to return
   */
  @SuppressWarnings("unchecked")
  private void processWaitEvents(boolean finalize)
  {
    Iterator<Map.Entry<JoinEvent<K,T>, Future<List>>> waitIterator = waitingEvents.entrySet().iterator();
    while (waitIterator.hasNext()) {
      Map.Entry<JoinEvent<K,T>, Future<List>> waitingEvent = waitIterator.next();
      Future<List> future = waitingEvent.getValue();
      if (future.isDone() || finalize) {
        try {
          JoinEvent<K,T> event = waitingEvent.getKey();
          joinStream(event.value,event.isStream1Data,future.get());
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException("end window", e);
        }
        waitIterator.remove();
        if (!finalize) {
          break;
        }
      }
    }
  }

  @Override
  public void endWindow()
  {
    processWaitEvents(true);
    super.endWindow();
  }

  @Override
  public void teardown()
  {
    super.teardown();
  }

  /**
   * Return the number of buckets
   * @return the noOfBuckets
   */
  public int getNoOfBuckets()
  {
    return noOfBuckets;
  }

  /**
   * Set the number of buckets required for managed state
   * @param noOfBuckets noOfBuckets
   */
  public void setNoOfBuckets(int noOfBuckets)
  {
    this.noOfBuckets = noOfBuckets;
  }

  /**
   * Return the bucketSpanTime
   * @return the bucketSpanTime
   */
  public Long getBucketSpanTime()
  {
    return bucketSpanTime;
  }

  /**
   * Sets the length of the time bucket required for managed state.
   * @param bucketSpanTime given bucketSpanTime
   */
  public void setBucketSpanTime(Long bucketSpanTime)
  {
    this.bucketSpanTime = bucketSpanTime;
  }

  public static class JoinEvent<K,T>
  {
    public K key;
    public T value;
    public boolean isStream1Data;

    public JoinEvent(K key, T value, boolean isStream1Data)
    {
      this.key = key;
      this.value = value;
      this.isStream1Data = isStream1Data;
    }
  }

  /**
   * Specify the Serde for the keys
   * @return Serde
   */
  public abstract Serde<K> getKeySerde();

  /**
   * Specify the Serde for the values
   * @return Serde
   */
  public abstract Serde<T> getValueSerde();

  public abstract TimeExtractor getTimeExtractor(boolean isStream1);

  public abstract KeyBucketExtractor getKeyBucketExtractor(boolean isStream1);

  /**
   * Return type of SpillableListMultimap to make as common interface for SpillableMap and SpillableArrayListMultiMap.
   * @param <K> Key type
   * @param <V> Value type
   */
  public class SpillableMapAsMultiMapImpl<K,V> implements Spillable.SpillableListMultimap<K, V>, Spillable.SpillableComponent
  {
    private SpillableTimeSlicedMapImpl<K,V> map;

    /**
     * Creates a {@link SpillableMapAsMultiMapImpl}
     * @param store The {@link SpillableTimeStateStore} in which to spill to.
     * @param identifier The Id of this {@link SpillableMapAsMultiMapImpl}.
     * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
     * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
     */
    public SpillableMapAsMultiMapImpl(SpillableTimeStateStore store, byte[] identifier, Serde<K> serdeKey, Serde<V> serdeValue)
    {
      map = new SpillableTimeSlicedMapImpl<>(store, identifier, serdeKey, serdeValue);
    }

    /**
     * Creates a {@link SpillableMapAsMultiMapImpl}
     * @param store The {@link SpillableTimeStateStore} in which to spill to.
     * @param identifier The Id of this {@link SpillableMapAsMultiMapImpl}.
     * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
     * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
     * @param timeExtractor Extract time from the each element and use it to decide where the data goes
     * @param keyBucketExtractor Extract key bucket id from each element and use it to descide where the data goes
     */
    public SpillableMapAsMultiMapImpl(SpillableTimeStateStore store, byte[] identifier, Serde<K> serdeKey, Serde<V> serdeValue,
        @NotNull TimeExtractor timeExtractor, @NotNull KeyBucketExtractor keyBucketExtractor)
    {
      map = new SpillableTimeSlicedMapImpl<>(store, identifier, serdeKey, serdeValue, timeExtractor, keyBucketExtractor);
    }

    /**
     * Creates a {@link SpillableMapAsMultiMapImpl}
     * @param store The {@link SpillableTimeStateStore} in which to spill to.
     * @param identifier The Id of this {@link SpillableMapAsMultiMapImpl}.
     * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
     * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
     * @param keyBucketExtractor Extract key bucket id from each element and use it to descide where the data goes
     */
    public SpillableMapAsMultiMapImpl(SpillableTimeStateStore store, byte[] identifier, Serde<K> serdeKey, Serde<V> serdeValue,
        @NotNull KeyBucketExtractor keyBucketExtractor)
    {
      map = new SpillableTimeSlicedMapImpl<>(store, identifier, serdeKey, serdeValue, keyBucketExtractor);
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      map.setup(context);
    }

    @Override
    public void teardown()
    {
      map.teardown();
    }

    @Override
    public List<V> get(@Nullable K k)
    {
      V value = map.get(k);
      if (value == null) {
        return null;
      }
      List<V> valueList = new ArrayList<>();
      valueList.add(value);
      return valueList;
    }

    @SuppressWarnings("unchecked")
    public Future<List<V>> getAsync(@Nullable K k)
    {
      return new CompositeFuture(map.getAsync(k));
    }

    @Override
    public Set<K> keySet()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Multiset<K> keys()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<V> values()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Map.Entry<K, V>> entries()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<V> removeAll(@Nullable Object o)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size()
    {
      return map.size();
    }

    @Override
    public boolean isEmpty()
    {
      return map.isEmpty();
    }

    @Override
    public boolean containsKey(@Nullable Object o)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(@Nullable Object o)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsEntry(@Nullable Object o, @Nullable Object o1)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean put(@Nullable K k, @Nullable V v)
    {
      map.put(k,v);
      return true;
    }

    @Override
    public boolean remove(@Nullable Object o, @Nullable Object o1)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean putAll(@Nullable K k, Iterable<? extends V> iterable)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean putAll(Multimap<? extends K, ? extends V> multimap)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<V> replaceValues(K k, Iterable<? extends V> iterable)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<K, Collection<V>> asMap()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void beginWindow(long windowId)
    {
      map.beginWindow(windowId);
    }

    @Override
    public void endWindow()
    {
      map.endWindow();
    }
  }

  /**
   * Converts the Future<V> to Future<List<V>>. This is used in map.getAsync()
   * @param <V> Value type
   */
  public class CompositeFuture<V> implements Future<List<V>>
  {
    public Future<V> valueFuture;

    public CompositeFuture(Future<V> valueFuture)
    {
      this.valueFuture = valueFuture;
    }

    @Override
    public boolean cancel(boolean b)
    {
      return valueFuture.cancel(b);
    }

    @Override
    public boolean isCancelled()
    {
      return valueFuture.isCancelled();
    }

    @Override
    public boolean isDone()
    {
      return valueFuture.isDone();
    }

    /**
     * Converts the single element into the list.
     * @return the list of values
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public List get() throws InterruptedException, ExecutionException
    {
      V value = valueFuture.get();
      if (value == null) {
        return null;
      }
      List<V> valueList = new ArrayList<>();
      valueList.add(value);
      return valueList;
    }

    @Override
    public List get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException
    {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * SpillableComplexComponent over SpillableTimeStateStore
   */
  public class SpillableTimeSlicedComplexComponentImpl implements SpillableComplexComponent
  {
    private List<SpillableComponent> componentList = Lists.newArrayList();

    @NotNull
    private SpillableTimeStateStore store;

    @NotNull
    private SpillableIdentifierGenerator identifierGenerator;

    /**
     * need to make sure all the buckets are created during setup.
     */
    protected transient Set<Long> bucketIds = Sets.newHashSet();

    private SpillableTimeSlicedComplexComponentImpl()
    {
      // for kryo
    }

    public SpillableTimeSlicedComplexComponentImpl(SpillableTimeStateStore store)
    {
      this(store, new SequentialSpillableIdentifierGenerator());
    }

    public SpillableTimeSlicedComplexComponentImpl(SpillableTimeStateStore store, SpillableIdentifierGenerator identifierGenerator)
    {
      this.store = Preconditions.checkNotNull(store);
      this.identifierGenerator = Preconditions.checkNotNull(identifierGenerator);
    }

    @Override
    public <T> SpillableList<T> newSpillableArrayList(long bucket, Serde<T> serde)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> SpillableList<T> newSpillableArrayList(byte[] identifier, long bucket, Serde<T> serde)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> SpillableMap<K, V> newSpillableMap(long bucket, Serde<K> serdeKey,
        Serde<V> serdeValue)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> SpillableMap<K, V> newSpillableMap(byte[] identifier, long bucket, Serde<K> serdeKey,
        Serde<V> serdeValue)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> SpillableMap<K, V> newSpillableMap(byte[] identifier, Serde<K> serdeKey, Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> SpillableMap<K, V> newSpillableMap(Serde<K> serdeKey,
        Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
    {
      throw new UnsupportedOperationException();
    }

    public <K, V> SpillableListMultimap<K, V> newSpillableMap(Serde<K> serdeKey,
        Serde<V> serdeValue, TimeExtractor<K> timeExtractor, KeyBucketExtractor keyBucketExtractor)
    {
      SpillableMapAsMultiMapImpl<K, V> map = timeExtractor != null ?
          new SpillableMapAsMultiMapImpl<>(store, identifierGenerator.next(), serdeKey, serdeValue, timeExtractor, keyBucketExtractor) :
          new SpillableMapAsMultiMapImpl<>(store, identifierGenerator.next(), serdeKey, serdeValue, keyBucketExtractor);
      componentList.add(map);
      return map;
    }

    public <K, V> SpillableListMultimap<K, V> newSpillableMap(byte[] identifier, Serde<K> serdeKey,
        Serde<V> serdeValue)
    {
      SpillableMapAsMultiMapImpl<K,V> map = new SpillableMapAsMultiMapImpl<>(store, identifier, serdeKey, serdeValue);
      componentList.add(map);
      return map;
    }

    public <K, V> SpillableListMultimap<K, V> newSpillableMap(Serde<K> serdeKey, Serde<V> serdeValue)
    {
      SpillableMapAsMultiMapImpl<K,V> map = new SpillableMapAsMultiMapImpl<>(store, identifierGenerator.next(), serdeKey, serdeValue);
      componentList.add(map);
      return map;
    }

    @Override
    public <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(long bucket, Serde<K> serdeKey, Serde<V> serdeValue)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(byte[] identifier, long bucket,
        Serde<K> serdeKey, Serde<V> serdeValue)
    {
      throw new UnsupportedOperationException();
    }

    public <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(byte[] identifier,
        Serde<K> serdeKey, Serde<V> serdeValue)
    {
      identifierGenerator.register(identifier);
      SpillableTimeSlicedArrayListMultiMapImpl<K, V> map = new SpillableTimeSlicedArrayListMultiMapImpl<>(store,
          identifier, serdeKey, serdeValue);
      componentList.add(map);
      return map;
    }

    public <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(Serde<K> serdeKey, Serde<V> serdeValue, TimeExtractor timeExtractor, KeyBucketExtractor keyBucketExtractor)
    {
      SpillableTimeSlicedArrayListMultiMapImpl<K, V> map = timeExtractor != null ?
          new SpillableTimeSlicedArrayListMultiMapImpl<>(store, identifierGenerator.next(), serdeKey, serdeValue, timeExtractor, keyBucketExtractor) :
          new SpillableTimeSlicedArrayListMultiMapImpl<>(store, identifierGenerator.next(), serdeKey, serdeValue, keyBucketExtractor);
      componentList.add(map);
      return map;
    }

    public <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(Serde<K> serdeKey, Serde<V> serdeValue)
    {
      SpillableTimeSlicedArrayListMultiMapImpl<K, V> map = new SpillableTimeSlicedArrayListMultiMapImpl<>(store,
          identifierGenerator.next(), serdeKey, serdeValue);
      componentList.add(map);
      return map;
    }

    @Override
    public <K, V> SpillableSetMultimap<K, V> newSpillableSetMultimap(long bucket, Serde<K> serdeKey, Serde<V> serdeValue)
    {
      throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public <K, V> SpillableSetMultimap<K, V> newSpillableSetMultimap(long bucket, Serde<K> serdeKey,
        Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
    {
      throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public <T> SpillableMultiset<T> newSpillableMultiset(long bucket, Serde<T> serde)
    {
      throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public <T> SpillableMultiset<T> newSpillableMultiset(byte[] identifier, long bucket, Serde<T> serde)
    {
      throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public <T> SpillableQueue<T> newSpillableQueue(long bucket, Serde<T> serde)
    {
      throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public <T> SpillableQueue<T> newSpillableQueue(byte[] identifier, long bucket, Serde<T> serde)
    {
      throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      store.setup(context);

      //ensure buckets created.
      for (long bucketId : bucketIds) {
        store.ensureBucket(bucketId);
      }

      //the bucket ids are only for setup. We don't need bucket ids during run time.
      bucketIds.clear();

      for (SpillableComponent spillableComponent: componentList) {
        spillableComponent.setup(context);
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      store.beginWindow(windowId);
      for (SpillableComponent spillableComponent: componentList) {
        spillableComponent.beginWindow(windowId);
      }
    }

    @Override
    public void endWindow()
    {
      for (SpillableComponent spillableComponent: componentList) {
        spillableComponent.endWindow();
      }
      store.endWindow();
    }

    @Override
    public void teardown()
    {
      for (SpillableComponent spillableComponent: componentList) {
        spillableComponent.teardown();
      }
      store.teardown();
    }

    @Override
    public void beforeCheckpoint(long l)
    {
      store.beforeCheckpoint(l);
    }

    @Override
    public void checkpointed(long l)
    {
      store.checkpointed(l);
    }

    @Override
    public void committed(long l)
    {
      store.committed(l);
    }
  }
}

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
package org.apache.apex.malhar.lib.window.impl;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;
import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.GenericSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.datatorrent.api.Context;

/**
 * This is an implementation of WindowedPlainStorage that makes use of {@link Spillable} data structures
 *
 * @param <T> Type of the value per window
 *
 * @since 3.6.0
 */
public class SpillableWindowedPlainStorage<T> implements WindowedStorage.WindowedPlainStorage<T>
{
  @NotNull
  private SpillableComplexComponent scc;
  private long bucket;
  private Serde<Window> windowSerde;
  private Serde<T> valueSerde;
  private static final Logger LOG = LoggerFactory.getLogger(SpillableWindowedPlainStorage.class);

  protected Spillable.SpillableMap<Window, T> windowToDataMap;

  public SpillableWindowedPlainStorage()
  {
  }

  public SpillableWindowedPlainStorage(long bucket, Serde<Window> windowSerde, Serde<T> valueSerde)
  {
    this.bucket = bucket;
    this.windowSerde = windowSerde;
    this.valueSerde = valueSerde;
  }

  public void setSpillableComplexComponent(SpillableComplexComponent scc)
  {
    this.scc = scc;
  }

  public SpillableComplexComponent getSpillableComplexComponent()
  {
    return scc;
  }

  public void setBucket(long bucket)
  {
    this.bucket = bucket;
  }

  public void setWindowSerde(Serde<Window> windowSerde)
  {
    this.windowSerde = windowSerde;
  }

  public void setValueSerde(Serde<T> valueSerde)
  {
    this.valueSerde = valueSerde;
  }

  @Override
  public void put(Window window, T value)
  {
    windowToDataMap.put(window, value);
  }

  @Override
  public T get(Window window)
  {
    return windowToDataMap.get(window);
  }

  @Override
  public Iterable<Map.Entry<Window, T>> entries()
  {
    return windowToDataMap.entrySet();
  }

  @Override
  public boolean containsWindow(Window window)
  {
    return windowToDataMap.containsKey(window);
  }

  @Override
  public long size()
  {
    return windowToDataMap.size();
  }

  @Override
  public void remove(Window window)
  {
    windowToDataMap.remove(window);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (bucket == 0) {
      // choose a bucket that is almost guaranteed to be unique
      bucket = (context.getValue(Context.DAGContext.APPLICATION_NAME) + "#" + context.getId()).hashCode();
    }
    // set default serdes
    if (windowSerde == null) {
      windowSerde = new GenericSerde<>();
    }
    if (valueSerde == null) {
      valueSerde = new GenericSerde<>();
    }
    if (windowToDataMap == null) {
      windowToDataMap = scc.newSpillableMap(windowSerde, valueSerde, new WindowTimeExtractor());
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void purge(long horizonMillis)
  {
    SpillableStateStore store = scc.getStore();
    if (store instanceof ManagedTimeUnifiedStateImpl) {
      ManagedTimeUnifiedStateImpl timeState = (ManagedTimeUnifiedStateImpl)store;
      long purgeTimeBucket = horizonMillis - timeState.getTimeBucketAssigner().getBucketSpan().getMillis();
      LOG.debug("Purging state less than equal to {}", purgeTimeBucket);
      timeState.purgeTimeBucketsLessThanEqualTo(purgeTimeBucket);
    }
  }
}

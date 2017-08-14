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

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;

/**
 * Wrapper class for TimeBased Store.
 *
 * @since 3.4.0
 */
@InterfaceStability.Unstable
public class InMemoryStore extends TimeBasedStore<TimeEvent> implements JoinStore
{
  public InMemoryStore()
  {
  }

  public InMemoryStore(long spanTimeInMillis, int bucketSpanInMillis)
  {
    super();
    setSpanTimeInMillis(spanTimeInMillis);
    setBucketSpanInMillis(bucketSpanInMillis);
  }

  @Override
  public void committed(long windowId)
  {

  }

  @Override
  public void checkpointed(long windowId)
  {

  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public List<TimeEvent> getUnMatchedTuples()
  {
    return super.getUnmatchedEvents();
  }

  @Override
  public void isOuterJoin(boolean isOuter)
  {
    super.isOuterJoin(isOuter);
  }

  @Override
  public List<TimeEvent> getValidTuples(Object tuple)
  {
    return super.getValidTuples((TimeEvent)tuple);
  }

  @Override
  public boolean put(Object tuple)
  {
    return super.put((TimeEvent)tuple);
  }

  private static final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

  @Override
  public void setup(Context context)
  {
    super.setup();
  }

  @Override
  public void teardown()
  {
    super.shutdown();
  }
}

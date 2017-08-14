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
package org.apache.apex.malhar.lib.dedup;

import java.util.Arrays;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateImpl;
import org.apache.apex.malhar.lib.state.managed.MovingBoundaryTimeBucketAssigner;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.netlet.util.Slice;

/**
 * An implementation for {@link AbstractDeduper} which handles the case of bounded data set.
 * This implementation assumes that the incoming tuple does not have a time field, and the de-duplication
 * is to be strictly based on the key of the tuple.
 *
 * This implementation uses {@link ManagedTimeStateImpl} for storing the tuple keys on the persistent storage.
 *
 * Following properties need to be configured for the functioning of the operator:
 * 1. {@link #keyExpression}: The java expression to extract the key fields in the incoming tuple (POJO)
 * 2. {@link #numBuckets} (optional): The number of buckets that need to be used for storing the keys of the
 * incoming tuples.
 * NOTE: Users can decide upon the proper value for this parameter by guessing the number of distinct keys
 * in the application. A appropriate value would be sqrt(num distinct keys). In case, the number of distinct keys is a
 * huge number, leave it blank so that the default value of 46340 will be used. The rationale for using this number is
 * that sqrt(max integer) = 46340. This implies that the number of buckets used will roughly be equal to the size of
 * each bucket, thus spreading the load equally among each bucket.
 *
 *
 * @since 3.5.0
 */
@Evolving
public class BoundedDedupOperator extends AbstractDeduper<Object>
{
  private static final long DEFAULT_CONSTANT_TIME = 0;
  private static final int DEFAULT_NUM_BUCKETS = 46340;

  // Required properties
  @NotNull
  private String keyExpression;

  //Optional, but recommended to be provided by user
  private int numBuckets = DEFAULT_NUM_BUCKETS;

  private transient Class<?> pojoClass;
  private transient Getter<Object, Object> keyGetter;
  private transient StreamCodec<Object> streamCodec;

  @InputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(PortContext context)
    {
      pojoClass = context.getAttributes().get(PortContext.TUPLE_CLASS);
      streamCodec = getDeduperStreamCodec();
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }

    @Override
    public StreamCodec<Object> getStreamCodec()
    {
      return streamCodec;
    }
  };

  public BoundedDedupOperator()
  {
    managedState = new ManagedTimeStateImpl();
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (numBuckets == 0) {
      numBuckets = DEFAULT_NUM_BUCKETS;
    }
    ((ManagedTimeStateImpl)managedState).setNumBuckets(numBuckets);
    MovingBoundaryTimeBucketAssigner timeBucketAssigner = new MovingBoundaryTimeBucketAssigner();
    managedState.setTimeBucketAssigner(timeBucketAssigner);
    super.setup(context);
  }

  @Override
  public void activate(Context context)
  {
    keyGetter = PojoUtils.createGetter(pojoClass, keyExpression, Object.class);
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  protected long getTime(Object tuple)
  {
    return DEFAULT_CONSTANT_TIME;
  }

  @Override
  protected Slice getKey(Object tuple)
  {
    Object key = keyGetter.get(tuple);
    return streamCodec.toByteArray(key);
  }

  protected StreamCodec<Object> getDeduperStreamCodec()
  {
    return new DeduperStreamCodec(keyExpression);
  }

  @Override
  protected Future<Slice> getAsyncManagedState(Object tuple)
  {
    Slice key = getKey(tuple);
    Future<Slice> valFuture = ((ManagedTimeStateImpl)managedState).getAsync(getBucketId(key), key);
    return valFuture;
  }

  @Override
  protected void putManagedState(Object tuple)
  {
    Slice key = getKey(tuple);
    ((ManagedTimeStateImpl)managedState).put(getBucketId(key), DEFAULT_CONSTANT_TIME, key, new Slice(null, 0, 0));
  }

  protected int getBucketId(Slice key)
  {
    return Arrays.hashCode(key.buffer) % numBuckets;
  }

  /**
   * Returns the key expression
   * @return key expression
   */
  public String getKeyExpression()
  {
    return keyExpression;
  }

  /**
   * Sets the key expression for the fields used for de-duplication
   * @param keyExpression the expression
   */
  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  /**
   * Returns the number of buckets
   * @return number of buckets
   */
  public int getNumBuckets()
  {
    return numBuckets;
  }

  /**
   * Sets the number of buckets
   * NOTE: Users can decide upon the proper value for this parameter by guessing the number of distinct keys
   * in the application. A appropriate value would be sqrt(num distinct keys). In case, the number of distinct keys is a
   * huge number, leave it blank so that the default value of 46340 will be used. The rationale for using this number is
   * that sqrt(max integer) = 46340. This implies that the number of buckets used will roughly be equal to the size of
   * each bucket, thus spreading the load equally among each bucket.
   * @param numBuckets the number of buckets
   */
  public void setNumBuckets(int numBuckets)
  {
    this.numBuckets = numBuckets;
  }
}

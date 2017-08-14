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

import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;
import org.apache.apex.malhar.lib.state.managed.MovingBoundaryTimeBucketAssigner;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.netlet.util.Slice;

/**
 * Time based deduper will de-duplicate incoming POJO tuples and classify them into the following:
 * 1. Unique
 * 2. Duplicate
 * 3. Expired
 *
 * Since this is de-duplicating in a stream of tuples, and we cannot store all incoming keys indefinitely,
 * we use the concept of expiry, where incoming tuples expire after a specified period of time. In this case,
 * we choose to expire an entire bucket of data as a unit. This requires the user to specify the bucketing
 * structure in advance in order for the operator to function. Here are the parameters for specifying the
 * bucketing structure:
 * 1. {@link #expireBefore} (in seconds)- This is the total time period during which a tuple stays in the
 * system and blocks any other tuple with the same key.
 * 2. {@link #bucketSpan} (in seconds) - This is the unit which describes how large a bucket can be.
 * Typically this should be defined depending on the use case. For example, if we have {@link #expireBefore}
 * set to 1 hour, then typically we would be clubbing data in the order of minutes, so a {@link #bucketSpan} of
 * around 1 minute or 5 minutes would make sense. Note that in this case, the entire data worth 1 minute or
 * 5 minutes will expire as a whole. Setting it to 1 minute would make the number of time buckets in the system
 * to be 1 hour / 1 minute = 60 buckets. Similarly setting {@link #bucketSpan} to 5 minutes would make number
 * of buckets to be 12. Note that having too many or too less buckets could have a performance impact. If unsure,
 * set the {@link #bucketSpan} to be ~ sqrt({@link #expireBefore}). This way the number of buckets and bucket span
 * are balanced.
 * 3. {@link #referenceInstant} - The reference point from which to start the time which is used for expiry.
 * Setting the {@link #referenceInstant} to say, r seconds from the epoch, would initialize the start of expiry
 * to be from that instant = r. The start and end of the expiry window periodically move by the span of a single
 * bucket. Refer {@link MovingBoundaryTimeBucketAssigner} for details.
 *
 * Additionally, it also needs the following parameters:
 * 1. {@link #keyExpression} - The java expression to extract the key fields in the incoming tuple (POJO)
 * 2. {@link #timeExpression} - The java expression to extract the time field in the incoming tuple (POJO).
 * In case there is no time field in the tuple, system time, when the tuple is processed, will be used.
 *
 *
 * @since 3.5.0
 */
@Evolving
public class TimeBasedDedupOperator extends AbstractDeduper<Object> implements ActivationListener<Context>
{

  // Required properties
  @NotNull
  private String keyExpression;

  @NotNull
  private String timeExpression;

  @NotNull
  private long bucketSpan;

  @NotNull
  private long expireBefore;

  // Optional
  private long referenceInstant = new Instant().getMillis() / 1000;

  private transient Class<?> pojoClass;

  private transient Getter<Object, Long> timeGetter;

  private transient Getter<Object, Object> keyGetter;

  private transient StreamCodec<Object> streamCodec;

  public TimeBasedDedupOperator()
  {
    managedState = new ManagedTimeUnifiedStateImpl();
  }

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

  @Override
  protected long getTime(Object tuple)
  {
    return timeGetter.get(tuple);
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
  public void setup(OperatorContext context)
  {
    MovingBoundaryTimeBucketAssigner timeBucketAssigner = new MovingBoundaryTimeBucketAssigner();
    timeBucketAssigner.setBucketSpan(Duration.standardSeconds(bucketSpan));
    timeBucketAssigner.setExpireBefore(Duration.standardSeconds(expireBefore));
    timeBucketAssigner.setReferenceInstant(new Instant(referenceInstant * 1000));
    managedState.setTimeBucketAssigner(timeBucketAssigner);
    super.setup(context);
  }

  @Override
  public void activate(Context context)
  {
    if (timeExpression != null) {
      timeGetter = PojoUtils.createGetter(pojoClass, timeExpression, Long.class);
    } else {
      timeGetter = null;
    }
    keyGetter = PojoUtils.createGetter(pojoClass, keyExpression, Object.class);
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  protected Future<Slice> getAsyncManagedState(Object tuple)
  {
    Future<Slice> valFuture = ((ManagedTimeUnifiedStateImpl)managedState).getAsync(getTime(tuple),
        getKey(tuple));
    return valFuture;
  }

  @Override
  protected void putManagedState(Object tuple)
  {
    ((ManagedTimeUnifiedStateImpl)managedState).put(getTime(tuple), getKey(tuple), new Slice(null, 0, 0));
  }


  public String getKeyExpression()
  {
    return keyExpression;
  }

  /**
   * Sets the key expression
   * @param keyExpression
   */
  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  public String getTimeExpression()
  {
    return timeExpression;
  }

  /**
   * Sets the time expression
   * @param timeExpression
   */
  public void setTimeExpression(String timeExpression)
  {
    this.timeExpression = timeExpression;
  }

  public long getBucketSpan()
  {
    return bucketSpan;
  }

  /**
   * Sets the length of a single time bucket (in seconds)
   * @param bucketSpan
   */
  public void setBucketSpan(long bucketSpan)
  {
    this.bucketSpan = bucketSpan;
  }

  public long getExpireBefore()
  {
    return expireBefore;
  }

  /**
   * Sets the expiry time (in seconds). Any event with time before this is considered to be expired.
   * @param expireBefore
   */
  public void setExpireBefore(long expireBefore)
  {
    this.expireBefore = expireBefore;
  }

  public long getReferenceInstant()
  {
    return referenceInstant;
  }

  /**
   * Sets the reference instant (in seconds from the epoch).
   * By default this is the time when the application is started.
   * @param referenceInstant
   */
  public void setReferenceInstant(long referenceInstant)
  {
    this.referenceInstant = referenceInstant;
  }

}

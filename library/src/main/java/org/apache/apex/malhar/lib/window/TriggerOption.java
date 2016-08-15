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
package org.apache.apex.malhar.lib.window;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.joda.time.Duration;

import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * This class describes how triggers should be fired for each window.
 * For each window, a trigger can be fired before the watermark (EARLY), at the watermark (ON_TIME), or after the watermark (LATE).
 * If a LATE trigger is specified and the accumulation mode is ACCUMULATING, it is important for the WindowOption to
 * specify the allowed lateness because otherwise, all states must be kept in storage.
 *
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class TriggerOption
{

  public enum AccumulationMode
  {
    DISCARDING,
    ACCUMULATING,
    ACCUMULATING_AND_RETRACTING
  }

  private AccumulationMode accumulationMode = AccumulationMode.DISCARDING;
  private boolean firingOnlyUpdatedPanes = false;

  /**
   * Whether the trigger should be fired before the watermark, at the watermark, or after the watermark
   */
  public enum Type
  {
    EARLY,
    ON_TIME,
    LATE
  }

  /**
   * This class represents the individual trigger spec.
   */
  public static class Trigger
  {
    protected Type type;

    private Trigger()
    {
      // for kryo
    }

    Trigger(Type type)
    {
      this.type = type;
    }

    public Type getType()
    {
      return type;
    }
  }

  /**
   * This class represents a trigger spec in which triggers are fired at regular time intervals.
   */
  public static class TimeTrigger extends Trigger
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    Duration duration;

    private TimeTrigger()
    {
      // for kryo
    }

    public TimeTrigger(Type type, Duration duration)
    {
      super(type);
      this.duration = duration;
    }

    public Duration getDuration()
    {
      return duration;
    }
  }

  /**
   * This class represents a trigger spec in which triggers are fired every n tuples
   */
  public static class CountTrigger extends Trigger
  {
    private long count;
    private CountTrigger()
    {
      //for kryo
    }

    public CountTrigger(Type type, long count)
    {
      super(type);
      this.count = count;
    }

    public long getCount()
    {
      return count;
    }
  }

  List<Trigger> triggerList = new ArrayList<>();

  /**
   * Creates a TriggerOption with an initial trigger that should be fired at the watermark
   *
   * @return the TriggerOption
   */
  public static TriggerOption AtWatermark()
  {
    TriggerOption triggerOption = new TriggerOption();
    Trigger trigger = new Trigger(Type.ON_TIME);
    triggerOption.triggerList.add(trigger);
    return triggerOption;
  }

  /**
   * A trigger should be fired before the watermark once for every specified duration
   *
   * @param duration the duration
   * @return the TriggerOption
   */
  public TriggerOption withEarlyFiringsAtEvery(Duration duration)
  {
    TimeTrigger trigger = new TimeTrigger(Type.EARLY, duration);
    triggerList.add(trigger);
    return this;
  }

  /**
   * A trigger should be fired before the watermark once for every n tuple(s)
   *
   * @param count the count
   * @return the TriggerOption
   */
  public TriggerOption withEarlyFiringsAtEvery(long count)
  {
    CountTrigger trigger = new CountTrigger(Type.EARLY, count);
    triggerList.add(trigger);
    return this;
  }

  /**
   * A trigger should be fired after the watermark once for every specified duration
   *
   * @param duration the duration
   * @return the TriggerOption
   */
  public TriggerOption withLateFiringsAtEvery(Duration duration)
  {
    TimeTrigger trigger = new TimeTrigger(Type.LATE, duration);
    triggerList.add(trigger);
    return this;
  }

  /**
   * A trigger should be fired after the watermark once for every n late tuple(s)
   *
   * @param count the count
   * @return the TriggerOption
   */
  public TriggerOption withLateFiringsAtEvery(long count)
  {
    CountTrigger trigger = new CountTrigger(Type.LATE, count);
    triggerList.add(trigger);
    return this;
  }

  /**
   * With discarding mode, the state is discarded after each trigger
   *
   * @return the TriggerOption
   */
  public TriggerOption discardingFiredPanes()
  {
    this.accumulationMode = AccumulationMode.DISCARDING;
    return this;
  }

  /**
   * With accumulating mode, the state is kept
   *
   * @return the TriggerOption
   */
  public TriggerOption accumulatingFiredPanes()
  {
    this.accumulationMode = AccumulationMode.ACCUMULATING;
    return this;
  }

  /**
   * With accumulating and retracting mode, the state is kept, and the snapshot of the state is saved after each trigger
   * so when new values come in that change the state, a retraction trigger can be fired with the snapshot of the state
   * when the last trigger was fired
   *
   * @return the TriggerOption
   */
  public TriggerOption accumulatingAndRetractingFiredPanes()
  {
    this.accumulationMode = AccumulationMode.ACCUMULATING_AND_RETRACTING;
    return this;
  }

  /**
   * Only fire triggers for data that has changed from the last trigger. This only applies to ACCUMULATING and
   * ACCUMULATING_AND_RETRACTING accumulation modes.
   *
   * @return the TriggerOption
   */
  public TriggerOption firingOnlyUpdatedPanes()
  {
    this.firingOnlyUpdatedPanes = true;
    return this;
  }

  /**
   * Gets the accumulation mode
   *
   * @return the AccumulationMode
   */
  public AccumulationMode getAccumulationMode()
  {
    return accumulationMode;
  }

  /**
   * Gets the trigger list
   *
   * @return the trigger list
   */
  public List<Trigger> getTriggerList()
  {
    return Collections.unmodifiableList(triggerList);
  }

  /**
   * Returns whether we should only fire panes that have been updated since the last trigger.
   * When this option is set, DISCARDING accumulation mode must not be used.
   *
   * @return whether we want to fire only updated panes
   */
  public boolean isFiringOnlyUpdatedPanes()
  {
    return this.firingOnlyUpdatedPanes;
  }
}

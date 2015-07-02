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
package com.datatorrent.lib.util;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This operator consumes tuples.&nbsp;
 * The operator only emits a tuple if,
 * at the time the operator receives the tuple,
 * the amount of time since the last alert interval is greater than the specified alert interval.
 * <p></p>
 * @displayName Alert Escalation
 * @category Algorithmic
 * @tags time, filter
 * @since 0.3.2
 */
@Deprecated
@OperatorAnnotation(partitionable=false)
public class AlertEscalationOperator extends BaseOperator
{
  protected long lastAlertTimeStamp = -1;
  protected long inAlertSince = -1;
  protected long lastTupleTimeStamp = -1;
  protected long timeout = 5000; // 5 seconds
  protected long alertInterval = 0;
  protected boolean activated = true;

  /**
   * This is the input port which receives tuples.
   */
  @InputPortFieldAnnotation(optional = false)
  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }

  };

  /**
   * This is the output port which emits a tuple when the alert criteria is met.
   */
  @OutputPortFieldAnnotation(optional = false)
  public final transient DefaultOutputPort<Object> alert = new DefaultOutputPort<Object>();

  public void processTuple(Object tuple)
  {
      long now = System.currentTimeMillis();
      if (inAlertSince < 0) {
          inAlertSince = now;
      }
      lastTupleTimeStamp = now;
      if (activated && (lastAlertTimeStamp < 0 || lastAlertTimeStamp + alertInterval < now)) {
          alert.emit(tuple);
          lastAlertTimeStamp = now;
      }

  }

  public long getTimeout()
  {
    return timeout;
  }

  public void setTimeout(long timeout)
  {
    this.timeout = timeout;
  }

  public long getAlertInterval()
  {
    return alertInterval;
  }

  /**
   * @param alertInterval time is in milliseconds
   */
  public void setAlertInterval(long alertInterval)
  {
    this.alertInterval = alertInterval;
  }

  public boolean isActivated()
  {
    return activated;
  }

  public void setActivated(boolean activated)
  {
    this.activated = activated;
  }

  @Override
  public void endWindow()
  {
    checkTimeout();
  }

  protected void checkTimeout()
  {
    if (System.currentTimeMillis() - lastTupleTimeStamp > timeout) {
      inAlertSince = -1;
      lastAlertTimeStamp = -1;
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    if(context != null) {
      context.getAttributes().put(OperatorContext.AUTO_RECORD, true);
    }
  }

}

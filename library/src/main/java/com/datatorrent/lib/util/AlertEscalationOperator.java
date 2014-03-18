/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * <p>AlertEscalationOperator class.</p>
 *
 * @since 0.3.2
 */
public class AlertEscalationOperator extends BaseOperator implements Partitioner<AlertEscalationOperator>
{
  protected long lastAlertTimeStamp = -1;
  protected long inAlertSince = -1;
  protected long lastTupleTimeStamp = -1;
  protected long timeout = 5000; // 5 seconds
  protected long alertInterval = 0;
  protected boolean activated = true;
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      long now = System.currentTimeMillis();
      if (inAlertSince < 0) {
        inAlertSince = now;
      }
      lastTupleTimeStamp = now;
      if (activated && (lastAlertTimeStamp < 0 || lastAlertTimeStamp + alertInterval < now)) {
        processTuple(tuple);
        lastAlertTimeStamp = now;
      }
    }

  };
  @OutputPortFieldAnnotation(name = "alert", optional = false)
  public final transient DefaultOutputPort<Object> alert = new DefaultOutputPort<Object>();

  public void processTuple(Object tuple)
  {
    alert.emit(tuple);
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
  public void partitioned(Map<Integer, Partition<AlertEscalationOperator>> partitions)
  {
  }

  @Override
  public Collection<Partition<AlertEscalationOperator>> definePartitions(Collection<Partition<AlertEscalationOperator>> partitions, int incrementalCapacity)
  {
    // prevent partitioning
    List<Partition<AlertEscalationOperator>> newPartitions = new ArrayList<Partition<AlertEscalationOperator>>(1);
    newPartitions.add(partitions.iterator().next());
    return newPartitions;
  }

  @Override
  public void setup(OperatorContext context)
  {
    if(context != null) {
      context.getAttributes().put(OperatorContext.AUTO_RECORD, true);
    }
  }

}

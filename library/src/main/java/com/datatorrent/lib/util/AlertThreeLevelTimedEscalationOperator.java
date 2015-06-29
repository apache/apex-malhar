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

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This operator allows the user to specify 3 different alert levels, each of which has its own output port.&nbsp;
 * If the amount of time between consecutive tuples exceeds the interval specified for an alert level,
 * then the tuple is emitted on the output port corresponding to that alert level.
 * <p></p>
 * @displayName Alert Three Level Timed Escalation
 * @category Algorithmic
 * @tags time, filter
 * @since 0.3.4
 */
@Deprecated
public class AlertThreeLevelTimedEscalationOperator extends AlertEscalationOperator
{
  protected long levelOneAlertTime = 0;
  protected long levelTwoAlertTime = 0;
  protected long levelThreeAlertTime = 0;

  /**
   * This is the output port that emits tuples when the alert level 2 criteria is met.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Object> alert2 = new DefaultOutputPort<Object>();

  /**
   * This is the output port that emits tuples when the alert level 3 criteria is met.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Object> alert3 = new DefaultOutputPort<Object>();

  @Override
  public void processTuple(Object tuple)
  {
	super.processTuple(tuple);
	  
    if (inAlertSince >= levelOneAlertTime) {
      alert.emit(tuple);
    }
    if (inAlertSince >= levelTwoAlertTime) {
      alert2.emit(tuple);
    }
    if (inAlertSince >= levelThreeAlertTime) {
      alert3.emit(tuple);
    }
  }

  public long getLevelOneAlertTime()
  {
    return levelOneAlertTime;
  }

  public void setLevelOneAlertTime(long levelOneAlertTime)
  {
    this.levelOneAlertTime = levelOneAlertTime;
  }

  public long getLevelTwoAlertTime()
  {
    return levelTwoAlertTime;
  }

  public void setLevelTwoAlertTime(long levelTwoAlertTime)
  {
    this.levelTwoAlertTime = levelTwoAlertTime;
  }

  public long getLevelThreeAlertTime()
  {
    return levelThreeAlertTime;
  }

  public void setLevelThreeAlertTime(long levelThreeAlertTime)
  {
    this.levelThreeAlertTime = levelThreeAlertTime;
  }

}

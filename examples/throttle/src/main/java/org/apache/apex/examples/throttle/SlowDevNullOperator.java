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

package org.apache.apex.examples.throttle;

import com.google.common.base.Throwables;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by pramod on 9/27/16.
 *
 * @since 3.7.0
 */
public class SlowDevNullOperator<T> extends BaseOperator
{
  // Modify sleep time dynamically while app is running to increase and decrease sleep time
  long sleepTime = 1;

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      // Introduce an artificial delay for every tuple
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  public long getSleepTime()
  {
    return sleepTime;
  }

  public void setSleepTime(long sleepTime)
  {
    this.sleepTime = sleepTime;
  }
}

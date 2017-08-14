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
package org.apache.apex.malhar.lib.bandwidth;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;

/**
 * BandwidthManager keeps track of bandwidth consumption and provides limit on maximum bandwidth that can be consumed at
 * any moment. This accumulates bandwidth upto certain limits so that accumulated bandwidth can be used over a period of
 * time.
 *
 * @since 3.5.0
 */
public class BandwidthManager implements Component<Context.OperatorContext>
{
  private static final Logger LOG = LoggerFactory.getLogger(BandwidthManager.class);
  /**
   * Maximum bandwidth that can be consumed in bytes/sec
   */
  private long bandwidthLimit = Long.MAX_VALUE;
  private transient long currentBandwidthConsumption;
  private final transient ScheduledExecutorService scheduler;
  private final transient Object lock = new Object();

  public BandwidthManager()
  {
    scheduler = Executors.newScheduledThreadPool(1);
  }

  BandwidthManager(ScheduledExecutorService scheduler)
  {
    this.scheduler = scheduler;
  }

  @Override
  public void setup(OperatorContext context)
  {
    scheduler.scheduleAtFixedRate(new Runnable()
    {
      @Override
      public void run()
      {
        if (isBandwidthRestricted()) {
          synchronized (lock) {
            if (currentBandwidthConsumption < 0) {
              currentBandwidthConsumption += bandwidthLimit;
            } else {
              currentBandwidthConsumption = bandwidthLimit;
            }
            LOG.debug("Available bandwidth: " + currentBandwidthConsumption);
          }
        }
      }
    }, 1, 1, TimeUnit.SECONDS);
  }

  public boolean canConsumeBandwidth()
  {
    if (!isBandwidthRestricted()) {
      return true;
    }
    synchronized (lock) {
      return currentBandwidthConsumption >= 0;
    }
  }

  public void consumeBandwidth(long sentTupleSize)
  {
    if (isBandwidthRestricted()) {
      synchronized (lock) {
        currentBandwidthConsumption -= sentTupleSize;
      }
    }
  }

  public boolean isBandwidthRestricted()
  {
    return bandwidthLimit != Long.MAX_VALUE;
  }

  /**
   * get maximum bandwidth that can be consumed in bytes/sec
   *
   * @return
   */
  public long getBandwidth()
  {
    return bandwidthLimit;
  }

  /**
   * Set maximum bandwidth that can be consumed in bytes/sec
   *
   * @param bandwidth
   */
  public void setBandwidth(long bandwidth)
  {
    this.bandwidthLimit = bandwidth;
    LOG.debug("Bandwidth limit is set to: " + bandwidth + " bytes/sec");
  }

  @Override
  public void teardown()
  {
    scheduler.shutdownNow();
  }

}

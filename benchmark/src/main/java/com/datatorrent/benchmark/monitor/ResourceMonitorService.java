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
package com.datatorrent.benchmark.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceMonitorService
{
  public static ResourceMonitorService create(long period)
  {
    return new ResourceMonitorService(period);
  }

  private long period;
  private ScheduledExecutorService executorService;

  private ResourceMonitorService()
  {
    this(1000);
  }

  private ResourceMonitorService(long period)
  {
    this.period = period;
  }

  public ResourceMonitorService start()
  {
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(new LogGarbageCollectionTimeTask(period), 0, period, TimeUnit.MILLISECONDS);
    executorService.scheduleAtFixedRate(new LogSystemResourceTask(), 0, period, TimeUnit.MILLISECONDS);

    return this;
  }

  public void shutdownNow()
  {
    executorService.shutdownNow();
  }

  public static class LogGarbageCollectionTimeTask implements Runnable
  {
    private static final Logger logger = LoggerFactory.getLogger(LogGarbageCollectionTimeTask.class);

    private long period;
    private long lastGarbageCollectionTime = 0;

    public LogGarbageCollectionTimeTask(long period)
    {
      this.period = period;
    }

    @Override
    public void run()
    {
      long garbageCollectionTime = SystemResourceMonitor.create().getGarbageCollectionTime();
      logger.info("Garbage Collection Time: total: {}; This period: {}; ratio: {}%", garbageCollectionTime,
          garbageCollectionTime - lastGarbageCollectionTime,
          (garbageCollectionTime - lastGarbageCollectionTime) * 100 / period);
      lastGarbageCollectionTime = garbageCollectionTime;
    }
  }

  public static class LogSystemResourceTask implements Runnable
  {
    private static final Logger logger = LoggerFactory.getLogger(LogSystemResourceTask.class);
    private SystemResourceMonitor allResourceMonitor = SystemResourceMonitor.create(SystemResourceMonitor.allResources);

    @Override
    public void run()
    {
      logger.info("System Resource: {}", allResourceMonitor.getFormattedSystemResourceUsage());
    }
  }
}

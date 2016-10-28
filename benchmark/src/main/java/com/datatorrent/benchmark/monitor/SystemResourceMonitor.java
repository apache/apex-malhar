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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SystemResourceMonitor
{
  public static enum SystemResource
  {
    /**
     * memory
     */
    CommittedVirtualMemorySize, FreePhysicalMemorySize, TotalPhysicalMemorySize, TotalSwapSpaceSize, FreeSwapSpaceSize,

    /**
     * cpu
     */
    ProcessCpuTime, SystemCpuLoad, ProcessCpuLoad,

    /**
     * file
     */
    OpenFileDescriptorCount, MaxFileDescriptorCount
  }

  public static SystemResource[] memoryResources = new SystemResource[] {
      SystemResource.CommittedVirtualMemorySize,
      SystemResource.FreePhysicalMemorySize,
      SystemResource.TotalPhysicalMemorySize,
      SystemResource.TotalSwapSpaceSize,
      SystemResource.FreeSwapSpaceSize};

  public static SystemResource[] cpuResources = new SystemResource[] {
      SystemResource.ProcessCpuTime,
      SystemResource.SystemCpuLoad,
      SystemResource.ProcessCpuLoad};

  public static SystemResource[] fileResources = new SystemResource[] {
      SystemResource.OpenFileDescriptorCount,
      SystemResource.MaxFileDescriptorCount};

  public static SystemResource[] allResources = SystemResource.values();

  private static Map<Set<SystemResource>, SystemResourceMonitor> resourcesToMonitors = Maps.newHashMap();

  private static SystemResourceMonitor defaultInstance;

  public static SystemResourceMonitor create()
  {
    if (defaultInstance == null) {
      synchronized (SystemResourceMonitor.class) {
        if (defaultInstance == null) {
          defaultInstance = new SystemResourceMonitor();
        }
      }
    }
    return defaultInstance;
  }

  public static SystemResourceMonitor create(SystemResource[] systemResources)
  {
    SystemResourceMonitor monitor = resourcesToMonitors.get(Sets.newHashSet(systemResources));
    if (monitor == null) {
      synchronized (SystemResourceMonitor.class) {
        if (monitor == null) {
          monitor = new SystemResourceMonitor(systemResources);
          resourcesToMonitors.put(Sets.newHashSet(systemResources), monitor);
        }
      }
    }
    return monitor;
  }

  private SystemResource[] systemResources;
  private OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
  private Map<SystemResource, Number> systemResourceUsage = Maps.newEnumMap(SystemResource.class);
  private List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();

  private SystemResourceMonitor()
  {
  }

  private SystemResourceMonitor(SystemResource[] systemResources)
  {
    if (systemResources == null || systemResources.length == 0) {
      throw new IllegalArgumentException("Resources should not null or empty");
    }
    this.systemResources = systemResources;
  }

  public Map<SystemResource, Number> getSystemResourceUsage()
  {
    systemResourceUsage.clear();
    for (SystemResource sr : systemResources) {
      Method getterMethod = getGetterMethod(sr);
      getterMethod.setAccessible(true);
      try {
        systemResourceUsage.put(sr, (Number)getterMethod.invoke(operatingSystemMXBean));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
    return systemResourceUsage;
  }

  public String getAppName()
  {
    return operatingSystemMXBean.getName();
  }

  public String getFormattedSystemResourceUsage()
  {
    Map<SystemResource, Number> usage = getSystemResourceUsage();
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<SystemResource, Number> entry : usage.entrySet()) {
      formatEntry(entry, sb);
    }
    return sb.toString();
  }

  private void formatEntry(Map.Entry<SystemResource, Number> entry, StringBuilder sb)
  {
    sb.append("\n\t").append(entry.getKey()).append(": ").append(NumberFormat.getInstance().format(entry.getValue()));
  }

  private Method getGetterMethod(SystemResource sr)
  {
    try {
      return operatingSystemMXBean.getClass().getMethod("get" + sr.name(), null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public long getGarbageCollectionTime()
  {
    long time = 0;
    for (GarbageCollectorMXBean collector : garbageCollectorMXBeans) {
      time += collector.getCollectionTime();
    }
    return time;
  }
}

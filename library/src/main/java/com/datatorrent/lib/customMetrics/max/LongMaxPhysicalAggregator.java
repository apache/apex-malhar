/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.customMetrics.max;

import java.io.Serializable;
import java.util.Collection;

import com.datatorrent.api.CustomMetrics;

public class LongMaxPhysicalAggregator implements CustomMetrics.PhysicalAggregator<Long>, Serializable
{
  @Override
  public Long aggregate(Collection<Object> metrics)
  {
    Long max = null;
    for (Object metric : metrics) {
      long value = ((Number) metric).longValue();
      if (max == null || value > max) {
        max = value;
      }
    }
    return max;
  }

  private static final long serialVersionUID = 201504081325L;
}

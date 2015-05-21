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

package com.datatorrent.contrib.dimensions;

import com.datatorrent.contrib.hdht.HDHTReader.HDSQuery;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;

import java.util.List;
import java.util.Map;

public class QueryMeta {
  private List<Map<String, HDSQuery>> hdsQueries;
  private List<Map<String, EventKey>> eventKeys;

  public QueryMeta()
  {
  }

  /**
   * @return the hdsQueries
   */
  public List<Map<String, HDSQuery>> getHdsQueries()
  {
    return hdsQueries;
  }

  /**
   * @param hdsQueries the hdsQueries to set
   */
  public void setHdsQueries(List<Map<String, HDSQuery>> hdsQueries)
  {
    this.hdsQueries = hdsQueries;
  }

  /**
   * @return the event keys
   */
  public List<Map<String, EventKey>> getEventKeys()
  {
    return eventKeys;
  }

  /**
   * @param eventKeys event keys to set
   */
  public void setEventKeys(List<Map<String, EventKey>> eventKeys)
  {
    this.eventKeys = eventKeys;
  }

}

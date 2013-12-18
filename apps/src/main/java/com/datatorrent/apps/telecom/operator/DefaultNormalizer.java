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
package com.datatorrent.apps.telecom.operator;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
* This implements the default implementation for normalization
* @since 0.9.2
*/
public class DefaultNormalizer implements EnricherInterface<String,Map<String,Object>,String,Object>
{

  /**
   * This stores the map object storing the normalization values;
   */
  private Map<String,Map<String,Object>> prop;
  /**
   * This stores the key set for the above map
   */
  private Set<String> keySet;

  @Override
  public void configure(Map<String,Map<String,Object>> prop)
  {
    this.prop = prop;
    keySet = this.prop.keySet();
  }

  @Override
  public void enrichRecord(Map<String,Object> m)
  {
    Iterator<String> itr = keySet.iterator();
    String key;
    while (itr.hasNext()) {
      key = itr.next();
      if (m.containsKey(key)) {
        Object val = m.get(key);
        if (prop.get(key).get(val) != null) {
          m.put(key, prop.get(key).get(val));
        }
      }
    }

  }

}

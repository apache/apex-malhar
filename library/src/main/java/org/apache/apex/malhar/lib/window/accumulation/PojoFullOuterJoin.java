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
package org.apache.apex.malhar.lib.window.accumulation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Full outer join Accumulation for Pojo Streams.
 *
 */
@InterfaceStability.Evolving
public class PojoFullOuterJoin<InputT1, InputT2>
    extends AbstractPojoJoin<InputT1, InputT2>
{
  public PojoFullOuterJoin()
  {
   super();
  }

  public PojoFullOuterJoin(int num, Class<?> outClass, String... keys)
  {
    super(outClass,keys);
  }

  @Override
  public void addNonMatchingResult(List result, Map requiredMap, Set nullFields)
  {
    for (Object field : nullFields) {
      if (!keySetStream2.contains(field)) {
        requiredMap.put(field.toString(), null);
      }
    }
    result.add(requiredMap);
  }

  @Override
  public int getLeftStreamIndex()
  {
    return 0;
  }

  @Override
  public List<Map<String, Object>> getAllCombo(int streamIndex, List accu, Map curMap)
  {
    List<Map<String, Object>> result = new ArrayList<>();
    int leftStreamIndex = getLeftStreamIndex();
    List<Map<String, Object>> leftStream = (List<Map<String, Object>>)accu.get(leftStreamIndex);
    List<Map<String, Object>> rightStream = (List<Map<String, Object>>)accu.get((leftStreamIndex + 1) % 2);
    Set<Map<String,Object>> unMatchedRightStream = new HashSet<>();

    for (Map<String, Object> rMap : rightStream) {
      unMatchedRightStream.add(rMap);
    }
    for (Map<String, Object> lMap : leftStream) {
      boolean gotMatch = false;
      for (Map<String, Object> rMap : rightStream) {
        Map<String, Object> tempMap = joinTwoMapsWithKeys(lMap, rMap);
        if (tempMap != null) {
          result.add(tempMap);
          gotMatch = true;
          if (unMatchedRightStream.contains(rMap)) {
            unMatchedRightStream.remove(rMap);
          }
        }
      }
      if (!gotMatch) {
        addNonMatchingResult(result, lMap, rightStream.get(0).keySet());
      }
    }

    for (Map<String, Object> rMap : unMatchedRightStream) {
      for (Object field : leftStream.get(0).keySet()) {
        if (!keySetStream1.contains(field)) {
          rMap.put(field.toString(), null);
        }
      }
      result.add(rMap);
    }
    return result;
  }
}

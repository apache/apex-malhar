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

import java.lang.reflect.Field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.window.MergeAccumulation;

import com.google.common.base.Throwables;

/**
 * Inner join Accumulation for Pojo Streams.
 *
 * @since 3.6.0
 */
public class PojoInnerJoin<InputT1, InputT2>
    implements MergeAccumulation<InputT1, InputT2, List<List<Map<String, Object>>>, List<?>>
{
  protected final String[] keys;
  protected final Class<?> outClass;

  public PojoInnerJoin()
  {
    keys = new String[]{};
    outClass = null;
  }

  public PojoInnerJoin(int num, Class<?> outClass, String... keys)
  {
    if (keys.length != 2) {
      throw new IllegalArgumentException("Wrong number of keys.");
    }

    this.keys = Arrays.copyOf(keys, keys.length);
    this.outClass = outClass;
  }

  @Override
  public List<List<Map<String, Object>>> accumulate(List<List<Map<String, Object>>> accumulatedValue, InputT1 input)
  {
    try {
      return accumulateWithIndex(0, accumulatedValue, input);
    } catch (NoSuchFieldException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<List<Map<String, Object>>> accumulate2(List<List<Map<String, Object>>> accumulatedValue, InputT2 input)
  {
    try {
      return accumulateWithIndex(1, accumulatedValue, input);
    } catch (NoSuchFieldException e) {
      throw Throwables.propagate(e);
    }
  }


  @Override
  public List<List<Map<String, Object>>> defaultAccumulatedValue()
  {
    List<List<Map<String, Object>>> accu = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      accu.add(new ArrayList<Map<String, Object>>());
    }
    return accu;
  }


  private List<List<Map<String, Object>>> accumulateWithIndex(int index, List<List<Map<String, Object>>> accu, Object input) throws NoSuchFieldException
  {
    // TODO: If a stream never sends out any tuple during one window, a wrong key would not be detected.

    input.getClass().getDeclaredField(keys[index]);

    List<Map<String, Object>> curList = accu.get(index);
    Map map = pojoToMap(input);
    curList.add(map);
    accu.set(index, curList);

    return accu;
  }

  private Map<String, Object> pojoToMap(Object input)
  {
    Map<String, Object> map = new HashMap<>();

    Field[] fields = input.getClass().getDeclaredFields();

    for (Field field : fields) {
      String[] words = field.getName().split("\\.");
      String fieldName = words[words.length - 1];
      field.setAccessible(true);
      try {
        Object value = field.get(input);
        map.put(fieldName, value);
      } catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      }
    }
    return map;
  }

  @Override
  public List<List<Map<String, Object>>> merge(List<List<Map<String, Object>>> accumulatedValue1, List<List<Map<String, Object>>> accumulatedValue2)
  {
    for (int i = 0; i < 2; i++) {
      List<Map<String, Object>> curList = accumulatedValue1.get(i);
      curList.addAll(accumulatedValue2.get(i));
      accumulatedValue1.set(i, curList);
    }
    return accumulatedValue1;
  }

  @Override
  public List<?> getOutput(List<List<Map<String, Object>>> accumulatedValue)
  {
    List<Map<String, Object>> result = new ArrayList<>();

    // TODO: May need to revisit (use state manager).
    result = getAllCombo(0, accumulatedValue, result, null);

    List<Object> out = new ArrayList<>();
    for (Map<String, Object> resultMap : result) {
      Object o;
      try {
        o = outClass.newInstance();
      } catch (Throwable e) {
        throw Throwables.propagate(e);
      }

      for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
        Field f;
        try {
          f = outClass.getDeclaredField(entry.getKey());
          f.setAccessible(true);
          f.set(o, entry.getValue());
        } catch (NoSuchFieldException | IllegalAccessException e) {
          throw Throwables.propagate(e);
        }
      }
      out.add(o);
    }

    return out;
  }


  private List<Map<String, Object>> getAllCombo(int streamIndex, List<List<Map<String, Object>>> accu, List<Map<String, Object>> result, Map<String, Object> curMap)
  {
    if (streamIndex == 2) {
      if (curMap != null) {
        result.add(curMap);
      }
      return result;
    } else {
      for (Map<String, Object> map : accu.get(streamIndex)) {
        if (streamIndex == 0) {
          Map<String, Object> tempMap = new HashMap<>(map);
          result = getAllCombo(streamIndex + 1, accu, result, tempMap);
        } else if (curMap == null) {
          return result;
        } else {
          Map<String, Object> tempMap = new HashMap<>(curMap);
          tempMap = joinTwoMapsWithKeys(tempMap, keys[0], map, keys[streamIndex]);
          result = getAllCombo(streamIndex + 1, accu, result, tempMap);
        }
      }
      return result;
    }
  }

  private Map<String, Object> joinTwoMapsWithKeys(Map<String, Object> map1, String key1, Map<String, Object> map2, String key2)
  {
    if (!map1.get(key1).equals(map2.get(key2))) {
      return null;
    } else {
      for (String field : map2.keySet()) {
        if (!field.equals(key2)) {
          map1.put(field, map2.get(field));
        }
      }
      return map1;
    }
  }

  @Override
  public List<?> getRetraction(List<?> value)
  {
    return null;
  }
}

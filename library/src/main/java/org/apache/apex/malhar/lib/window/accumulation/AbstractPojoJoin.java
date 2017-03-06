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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.apex.malhar.lib.window.MergeAccumulation;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Throwables;

import com.datatorrent.lib.util.PojoUtils;

/**
 * Join Accumulation for Pojo Streams.
 *
 */
@InterfaceStability.Evolving
public abstract class AbstractPojoJoin<InputT1, InputT2>
    implements MergeAccumulation<InputT1, InputT2, List<List<Map<String, Object>>>, List<?>>
{
  protected String[] keys;
  protected Class<?> outClass;
  private transient Map<String,PojoUtils.Getter> gettersStream1;
  private transient Map<String,PojoUtils.Getter> gettersStream2;
  private transient Map<String,PojoUtils.Setter> setters;
  protected transient Set<String> keySetStream2;
  protected transient Set<String> keySetStream1;
  protected transient String[] leftKeys;
  protected transient String[] rightKeys;

  public AbstractPojoJoin()
  {
    leftKeys = new String[]{};
    rightKeys = new String[]{};
    outClass = null;
  }

  public AbstractPojoJoin(Class<?> outClass, String[] leftKeys, String[] rightKeys)
  {
    if (leftKeys.length != rightKeys.length) {
      throw new IllegalArgumentException("Number of keys in left stream should match in right stream");
    }
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.outClass = outClass;
  }


  private void createSetters()
  {
    Field[] fields = outClass.getDeclaredFields();
    setters = new HashMap<>();
    for (Field field : fields) {
      Class outputField = ClassUtils.primitiveToWrapper(field.getType());
      String fieldName = field.getName();
      setters.put(fieldName,PojoUtils.createSetter(outClass,fieldName,outputField));
    }
  }

  private Map<String,PojoUtils.Getter> createGetters(Class<?> input)
  {
    Field[] fields = input.getDeclaredFields();
    Map<String,PojoUtils.Getter> getters = new HashMap<>();
    for (Field field : fields) {
      Class inputField = ClassUtils.primitiveToWrapper(field.getType());
      String fieldName = field.getName();
      getters.put(fieldName,PojoUtils.createGetter(input, fieldName, inputField));
    }
    return getters;
  }

  @Override
  public List<List<Map<String, Object>>> accumulate(List<List<Map<String, Object>>> accumulatedValue, InputT1 input)
  {
    if (gettersStream1 == null) {
      gettersStream1 = createGetters(input.getClass());
    }
    try {
      return accumulateWithIndex(0, accumulatedValue, input);
    } catch (NoSuchFieldException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<List<Map<String, Object>>> accumulate2(List<List<Map<String, Object>>> accumulatedValue, InputT2 input)
  {
    if (gettersStream2 == null) {
      gettersStream2 = createGetters(input.getClass());
    }
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

    List<Map<String, Object>> curList = accu.get(index);
    Map map = pojoToMap(input,index + 1);
    curList.add(map);
    accu.set(index, curList);

    return accu;
  }

  private Map<String, Object> pojoToMap(Object input, int streamIndex)
  {
    Map<String, Object> map = new HashMap<>();
    Map<String,PojoUtils.Getter> gettersStream = streamIndex == 1 ? gettersStream1 : gettersStream2;

    for (Map.Entry<String, PojoUtils.Getter> getter : gettersStream.entrySet()) {
      try {
        Object value = getter.getValue().get(input);
        map.put(getter.getKey(), value);
      } catch (Exception e) {
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
    if (setters == null) {
      createSetters();
      keySetStream2 = new HashSet<>();
      keySetStream1 = new HashSet<>();
      int lIndex = getLeftStreamIndex();
      for (int i = 0; i < leftKeys.length; i++) {
        keySetStream1.add(lIndex == 0 ? leftKeys[i] : rightKeys[i]);
        keySetStream2.add(lIndex == 1 ? leftKeys[i] : rightKeys[i]);
      }
    }

    // TODO: May need to revisit (use state manager).
    List<Map<String, Object>> result = getAllCombo(0, accumulatedValue, null);

    List<Object> out = new ArrayList<>();
    for (Map<String, Object> resultMap : result) {
      Object o;
      try {
        o = outClass.newInstance();
      } catch (Throwable e) {
        throw Throwables.propagate(e);
      }
      for (Map.Entry<String, PojoUtils.Setter> setter : setters.entrySet()) {
        if (resultMap.get(setter.getKey()) != null) {
          setter.getValue().set(o, resultMap.get(setter.getKey()));
        }
      }
      out.add(o);
    }

    return out;
  }


  public List<Map<String, Object>> getAllCombo(int streamIndex, List<List<Map<String, Object>>> accu, Map<String, Object> curMap)
  {
    List<Map<String, Object>> result = new ArrayList<>();
    int leftStreamIndex = getLeftStreamIndex();
    List<Map<String, Object>> leftStream = accu.get(leftStreamIndex);
    List<Map<String, Object>> rightStream = accu.get((leftStreamIndex + 1) % 2);
    for (Map<String, Object> lMap : leftStream) {
      boolean gotMatch = false;
      for (Map<String, Object> rMap : rightStream) {
        Map<String, Object> tempMap = joinTwoMapsWithKeys(lMap, rMap);
        if (tempMap != null) {
          result.add(tempMap);
          gotMatch = true;
        }
      }
      if (!gotMatch) {
        addNonMatchingResult(result, lMap, rightStream.get(0).keySet());
      }
    }
    return result;
  }

  public abstract void addNonMatchingResult(List<Map<String, Object>> result, Map<String, Object> requiredMap, Set nullFields);

  public abstract int getLeftStreamIndex();


  public Map<String, Object> joinTwoMapsWithKeys(Map<String, Object> map1, Map<String, Object> map2)
  {
    int lIndex = getLeftStreamIndex();
    for (int i = 0; i < leftKeys.length; i++) {
      String key1 = lIndex == 0 ? leftKeys[i] : rightKeys[i];
      String key2 = lIndex == 1 ? leftKeys[i] : rightKeys[i];
      if (!map1.get(key1).equals(map2.get(key2))) {
        return null;
      }
    }
    for (String field : map2.keySet()) {
      if (!keySetStream2.contains(field)) {
        map1.put(field, map2.get(field));
      }
    }
    return map1;
  }

  @Override
  public List<?> getRetraction(List<?> value)
  {
    return null;
  }
}

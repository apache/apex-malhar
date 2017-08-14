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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.window.MergeAccumulation;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import static org.apache.apex.malhar.lib.window.accumulation.AbstractPojoJoin.STREAM.LEFT;

/**
 * Join Accumulation for Pojo Streams.
 *
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public abstract class AbstractPojoJoin<InputT1, InputT2>
    implements MergeAccumulation<InputT1, InputT2, List<Multimap<List<Object>, Object>>, List<?>>
{
  protected String[] keys;
  protected Class<?> outClass;
  private transient Map<String,PojoUtils.Getter> gettersStream1;
  private transient Map<String,PojoUtils.Getter> gettersStream2;
  protected transient Map<String,PojoUtils.Setter> setters;
  protected transient Map<String, KeyValPair<STREAM, String>> outputToInputMap;
  protected transient String[] leftKeys;
  protected transient String[] rightKeys;
  public enum STREAM
  {
    LEFT, RIGHT
  }

  public AbstractPojoJoin()
  {
    leftKeys = new String[]{};
    rightKeys = new String[]{};
    outClass = null;
  }

  /**
   * This constructor will be used when the user wants to include all the fields of Output POJO
   * and the field names of output POJO match the field names of POJO coming on input streams.
   */
  public AbstractPojoJoin(Class<?> outClass, String[] leftKeys, String[] rightKeys)
  {
    if (leftKeys.length != rightKeys.length) {
      throw new IllegalArgumentException("Number of keys in left stream should match in right stream");
    }
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.outClass = outClass;
  }

  /**
   * This constructor will be used when the user wants to include some specific
   * fields of the output POJO and/or wants to have a mapping of the fields of output
   * POJO to POJO coming on input streams.
   */
  public AbstractPojoJoin(Class<?> outClass, String[] leftKeys, String[] rightKeys, Map<String, KeyValPair<STREAM, String>> outputToInputMap)
  {
    this(outClass,leftKeys,rightKeys);
    this.outputToInputMap = outputToInputMap;
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
  public List<Multimap<List<Object>, Object>> accumulate(List<Multimap<List<Object>, Object>> accumulatedValue, InputT1 input)
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
  public List<Multimap<List<Object>, Object>> accumulate2(List<Multimap<List<Object>, Object>> accumulatedValue, InputT2 input)
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
  public List<Multimap<List<Object>, Object>> defaultAccumulatedValue()
  {
    List<Multimap<List<Object>, Object>> accu = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Multimap<List<Object>, Object> mMap = ArrayListMultimap.create();
      accu.add(mMap);
    }
    return accu;
  }

  private List<Multimap<List<Object>, Object>>  accumulateWithIndex(int index,
      List<Multimap<List<Object>, Object>> accu, Object input) throws NoSuchFieldException
  {
    // TODO: If a stream never sends out any tuple during one window, a wrong key would not be detected.
    Multimap<List<Object>, Object> curMap = accu.get(index);
    List<Object> key = getKeyForMultiMap(input,index);
    curMap.put(key,input);
    return accu;
  }

  private List<Object> getKeyForMultiMap(Object input, int index)
  {
    List<Object> key = new ArrayList<>();
    String[] reqKeys = index == 0 ? leftKeys : rightKeys;
    Map<String,PojoUtils.Getter> gettersStream = index == 0 ? gettersStream1 : gettersStream2;
    for (String lkey : reqKeys ) {
      key.add(gettersStream.get(lkey).get(input));
    }
    return key;
  }

  @Override
  public List<Multimap<List<Object>, Object>> merge(List<Multimap<List<Object>, Object>> accumulatedValue1, List<Multimap<List<Object>, Object>> accumulatedValue2)
  {
    for (int i = 0; i < 2; i++) {
      Multimap<List<Object>, Object> mMap1 = accumulatedValue1.get(i);
      Multimap<List<Object>, Object> mMap2 = accumulatedValue2.get(i);
      for (Map.Entry<List<Object>, Object> entry : mMap2.entries()) {
        mMap1.put(entry.getKey(),entry.getValue());
      }
    }
    return accumulatedValue1;
  }

  @Override
  public List<?> getOutput(List<Multimap<List<Object>, Object>> accumulatedValue)
  {
    if (setters == null) {
      createSetters();
    }
    // TODO: May need to revisit (use state manager).
    return getAllCombo(accumulatedValue);
  }

  protected void setObjectForResult(Map<String,PojoUtils.Getter> stream, Object input, Object output)
  {
    for (Map.Entry<String, PojoUtils.Getter> getter : stream.entrySet()) {
      if (setters.containsKey(getter.getKey())) {
        setters.get(getter.getKey()).set(output, getter.getValue().get(input));
      }
    }

  }

  /**
   * This function takes the required join on the 2 input streams for matching keys
   * and allows the derived classes to implement the logic in case of non matching keys.
   *
   * It is designed such that for outer joins it will always assume that it is
   * a left outer join and hence it considers right stream as left in case of
   * right outer join keeping the code and logic the same.
   *
   * For each key in the left stream a corresponding key is searched in the right stream
   * if a match is found then the all the objects with that key are added to Output list,
   * also that key is removed from right stream as it will no longer be required in any join
   * scenario.If a match is not found then it relies on derived class implementation to handle it.
   *
   * @param accu which is the main accumulation data structure
   * @return List of objects got after joining the streams
   */
  private List<Object> getAllCombo(List<Multimap<List<Object>, Object>> accu)
  {
    List<Object> result = new ArrayList<>();
    int leftStreamIndex = getLeftStreamIndex();
    Multimap<List<Object>, Object> leftStream = accu.get(leftStreamIndex);
    Multimap<List<Object>, Object> rightStream = ArrayListMultimap.create(accu.get((leftStreamIndex + 1) % 2));
    Map<String,PojoUtils.Getter> leftGettersStream = leftStreamIndex == 0 ? gettersStream1 : gettersStream2;
    Map<String,PojoUtils.Getter> rightGettersStream = leftStreamIndex == 1 ? gettersStream1 : gettersStream2;
    for (List lMap : leftStream.keySet()) {
      Collection<Object> left = leftStream.get(lMap);
      if (rightStream.containsKey(lMap)) {
        Collection<Object> right = rightStream.get(lMap);
        Object o;
        try {
          o = outClass.newInstance();
        } catch (Throwable e) {
          throw Throwables.propagate(e);
        }
        for (Object lObj:left) {
          for (Object rObj:right) {
            if (outputToInputMap != null) {
              for (Map.Entry<String, KeyValPair<STREAM,String>> entry : outputToInputMap.entrySet()) {
                KeyValPair<STREAM,String> kv = entry.getValue();
                Object reqObject;
                Map<String,PojoUtils.Getter> reqStream;
                if (kv.getKey() == LEFT) {
                  reqObject = leftStreamIndex == 0 ? lObj : rObj;
                  reqStream = leftStreamIndex == 0 ? leftGettersStream : rightGettersStream;
                } else {
                  reqObject = leftStreamIndex == 0 ? rObj : lObj;
                  reqStream = leftStreamIndex == 0 ? rightGettersStream : leftGettersStream;
                }
                setters.get(entry.getKey()).set(o,reqStream.get(entry.getValue().getValue()).get(reqObject));
              }
            } else {
              setObjectForResult(leftGettersStream, lObj, o);
              setObjectForResult(rightGettersStream, rObj, o);
            }
          }
          result.add(o);
        }
        rightStream.removeAll(lMap);
      } else {
        addNonMatchingResult(left, leftGettersStream, result);
      }
    }
    addNonMatchingRightStream(rightStream, rightGettersStream, result);
    return result;
  }

  /**
   * This function defines the strategy to be used when no matching key is found.
   */
  protected abstract void addNonMatchingResult(Collection<Object> left, Map<String,PojoUtils.Getter> leftGettersStream, List<Object> result);

  /**
   * This function defines the strategy to be used when the join is interested to add POJO's
   * from right stream when no matching key is found.
   */
  protected abstract void addNonMatchingRightStream(Multimap<List<Object>, Object> rightStream, Map<String,PojoUtils.Getter> rightGettersStream, List<Object> result);

  /**
   * This function lets the join decide which is the left stream and which is the right stream.
   */
  protected abstract int getLeftStreamIndex();

  @Override
  public List<?> getRetraction(List<?> value)
  {
    return null;
  }
}

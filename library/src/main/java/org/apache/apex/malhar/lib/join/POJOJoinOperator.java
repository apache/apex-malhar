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
package org.apache.apex.malhar.lib.join;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class takes a POJO as input from each of the input port. Operator joines the input tuples
 * based on join constraint and emit the result.
 *
 * <br>
 * <b>Ports : </b> <br>
 * <b> input1 : </b> Input port for stream 1, expects POJO <br>
 * <b> input2 : </b> Input port for stream 2, expects POJO <br>
 * <b> outputPort: </b> Output port emits POJO <br>
 * <br>
 * <b>Example:</b>
 * Input tuple from port1 is
 * {timestamp = 5000, productId = 3, customerId = 108, regionId = 4, amount = $560 }
 *
 * Input tuple from port2 is
 * { timestamp = 5500, productCategory = 8, productId=3 }
 *
 * <b>Properties: </b>
 * <b>expiryTime</b>: 1000<br>
 * <b>includeFieldStr</b>: timestamp, customerId, amount; productCategory, productId<br>
 * <b>keyFields</b>: productId, productId<br>
 * <b>timeFields</b>: timestamp, timestamp<br>
 * <b>bucketSpanInMillis</b>: 500<br>
 *
 * <b>Output</b>
 * { timestamp = 5000, customerId = 108, amount = $560, productCategory = 8, productId=3}
 *
 * @displayName BeanJoin Operator
 * @category join
 * @tags join
 *
 * @since 3.4.0
 */
@InterfaceStability.Unstable
public class POJOJoinOperator extends AbstractJoinOperator
{
  protected Class outputClass;
  protected transient Class leftClass;
  protected transient Class rightClass;
  private String outputClassStr;
  private transient List<FieldObjectMap>[] fieldMap = (List<FieldObjectMap>[])Array.newInstance(
      (new LinkedList<FieldObjectMap>()).getClass(), 2);
  private transient PojoUtils.Getter[] keyGetters = (PojoUtils.Getter[])Array.newInstance(PojoUtils.Getter.class, 2);
  private transient PojoUtils.Getter[] timeGetters = (PojoUtils.Getter[])Array.newInstance(PojoUtils.Getter.class, 2);

  // Populate the getters from the input tuple
  @Override
  protected void processTuple(Object tuple)
  {
    setAndPopulateGetters(tuple, isLeft);
    super.processTuple(tuple);
  }

  /**
   * Populate the class and getters from the given tuple
   *
   * @param tuple Given tuple
   * @param isLeft Whether the given tuple belongs to left stream or not
   */
  private void setAndPopulateGetters(Object tuple, boolean isLeft)
  {
    if (isLeft && leftClass == null) {
      leftClass = tuple.getClass();
      populateGettersFromInput(isLeft);
    }
    if (!isLeft && rightClass == null) {
      rightClass = tuple.getClass();
      populateGettersFromInput(isLeft);
    }
  }

  /**
   * Populate the getters from the input class
   *
   * @param isLeft isLeft specifies whether the class is left or right
   */
  private void populateGettersFromInput(boolean isLeft)
  {
    Class inputClass;
    int idx;
    StoreContext store;
    if (isLeft) {
      idx = 0;
      inputClass = leftClass;
      store = leftStore;
    } else {
      idx = 1;
      inputClass = rightClass;
      store = rightStore;
    }
    String key = store.getKeys();
    String timeField = store.getTimeFields();
    String[] fields = store.getIncludeFields();

    // Create getter for the key field
    try {
      Class c = ClassUtils.primitiveToWrapper(inputClass.getField(key).getType());
      keyGetters[idx] = PojoUtils.createGetter(inputClass, key, c);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    // Create getter for time field
    if (timeField != null) {
      try {
        Class c = ClassUtils.primitiveToWrapper(inputClass.getField(timeField).getType());
        timeGetters[idx] = PojoUtils.createGetter(inputClass, timeField, c);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }

    fieldMap[idx] = new LinkedList<FieldObjectMap>();
    List<FieldObjectMap> fieldsMap = fieldMap[idx];
    // Create getters for the include fields
    for (String f : fields) {
      try {
        Field field = inputClass.getField(f);
        Class c;
        if (field.getType().isPrimitive()) {
          c = ClassUtils.primitiveToWrapper(field.getType());
        } else {
          c = field.getType();
        }
        FieldObjectMap fm = new FieldObjectMap();
        fm.get = PojoUtils.createGetter(inputClass, f, c);
        fm.set = PojoUtils.createSetter(outputClass, f, c);
        fieldsMap.add(fm);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to populate gettter for field: " + f, e);
      }
    }
  }

  /**
   * Create the output class object
   *
   * @return the new output object
   */
  @Override
  protected Object createOutputTuple()
  {
    try {
      return outputClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Copy the field values of extractTuple to output object
   *
   * @param output
   * @param extractTuple
   * @param isLeft
   */
  @Override
  protected void copyValue(Object output, Object extractTuple, boolean isLeft)
  {
    if (extractTuple == null) {
      return;
    }

    setAndPopulateGetters(extractTuple, isLeft);

    List<FieldObjectMap> fieldsMap;
    if (isLeft) {
      fieldsMap = fieldMap[0];
    } else {
      fieldsMap = fieldMap[1];
    }

    for (FieldObjectMap map : fieldsMap) {
      map.set.set(output, map.get.get(extractTuple));
    }
  }

  /**
   * Return the keyField value of tuple object
   *
   * @param keyField
   * @param tuple
   * @return the tuple value for the given keyfield
   */
  public Object getKeyValue(String keyField, Object tuple)
  {
    if (isLeft) {
      return keyGetters[0].get(tuple);
    }
    return keyGetters[1].get(tuple);
  }

  @Override
  protected Object getTime(String field, Object tuple)
  {
    if (getTimeFieldStr() != null) {
      if (isLeft) {
        return timeGetters[0].get(tuple);
      }
      return timeGetters[1].get(tuple);
    }
    return Calendar.getInstance().getTimeInMillis();
  }

  public void populateOutputClass()
  {
    try {
      this.outputClass = this.getClass().getClassLoader().loadClass(outputClassStr);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public String getOutputClass()
  {
    return outputClassStr;
  }

  /**
   * Load the output class
   *
   * @param outputClassStr
   */
  public void setOutputClass(String outputClassStr)
  {
    this.outputClassStr = outputClassStr;
    populateOutputClass();
  }

  private class FieldObjectMap
  {
    public PojoUtils.Getter get;
    public PojoUtils.Setter set;
  }
}

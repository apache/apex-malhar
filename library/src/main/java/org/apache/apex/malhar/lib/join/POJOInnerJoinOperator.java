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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.state.managed.KeyBucketExtractor;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.GenericSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.commons.lang3.ClassUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.PojoUtils;

/**
 * Concrete implementation of AbstractManagedStateInnerJoinOperator and receives objects from both streams.
 *
 * @displayName POJO Inner Join Operator
 * @tags join
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class POJOInnerJoinOperator extends AbstractManagedStateInnerJoinOperator<Object,Object> implements Operator.ActivationListener<Context>
{
  private transient FieldObjectMap[] inputFieldObjects = (FieldObjectMap[])Array.newInstance(FieldObjectMap.class, 2);
  protected transient Class<?> outputClass;

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      outputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> input1 = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      inputFieldObjects[0].inputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple,true);
    }

    @Override
    public StreamCodec<Object> getStreamCodec()
    {
      return getInnerJoinStreamCodec(true);
    }
  };

  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> input2 = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      inputFieldObjects[1].inputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple,false);
    }

    @Override
    public StreamCodec<Object> getStreamCodec()
    {
      return getInnerJoinStreamCodec(false);
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    for (int i = 0; i < 2; i++) {
      inputFieldObjects[i] = new FieldObjectMap();
    }
  }

  /**
   * Create getters for the key and time fields and setters for the include fields.
   */
  private void generateSettersAndGetters()
  {
    for (int i = 0; i < 2; i++) {
      Class inputClass = inputFieldObjects[i].inputClass;
      try {
        inputFieldObjects[i].keyGet = PojoUtils.createGetter(inputClass, keyFieldExpressions.get(i), Object.class);
        if (timeFields != null && timeFields.size() == 2) {
          Class timeField = ClassUtils.primitiveToWrapper(inputClass.getDeclaredField(timeFields.get(i)).getType());
          inputFieldObjects[i].timeFieldGet = PojoUtils.createGetter(inputClass, timeFields.get(i), timeField);
        }
        for (int j = 0; j < includeFields[i].length; j++) {
          Class inputField = ClassUtils.primitiveToWrapper(inputClass.getDeclaredField(includeFields[i][j]).getType());
          Class outputField = ClassUtils.primitiveToWrapper(outputClass.getDeclaredField(includeFields[i][j]).getType());
          if (inputField != outputField) {
            continue;
          }
          inputFieldObjects[i].fieldMap.put(PojoUtils.createGetter(inputClass, includeFields[i][j], inputField),
              PojoUtils.createSetter(outputClass, includeFields[i][j], outputField));
        }
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Extract the key value from the given tuple
   * @param tuple given tuple
   * @param isStream1Data Specifies whether the given tuple belongs to stream1 or not.
   * @return the key object
   */
  @SuppressWarnings("unchecked")
  @Override
  public Object extractKey(Object tuple, boolean isStream1Data)
  {
    return isStream1Data ? inputFieldObjects[0].keyGet.get(tuple) :
      inputFieldObjects[1].keyGet.get(tuple);
  }

  /**
   * Merge the given tuples
   * @param tuple1 tuple belongs to stream1
   * @param tuple2 tuple belongs to stream1
   * @return the merged output object
   */
  @SuppressWarnings("unchecked")
  @Override
  public Object mergeTuples(Object tuple1, Object tuple2)
  {
    Object o;
    try {
      o = outputClass.newInstance();
      for (Map.Entry<PojoUtils.Getter,PojoUtils.Setter> g: inputFieldObjects[0].fieldMap.entrySet()) {
        g.getValue().set(o, g.getKey().get(tuple1));
      }
      for (Map.Entry<PojoUtils.Getter,PojoUtils.Setter> g: inputFieldObjects[1].fieldMap.entrySet()) {
        g.getValue().set(o, g.getKey().get(tuple2));
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    return o;
  }

  /**
   * Emit the given tuple through the outputPort
   * @param tuple given tuple
   */
  @Override
  public void emitTuple(Object tuple)
  {
    outputPort.emit(tuple);
  }

  @Override
  public void activate(Context context)
  {
    generateSettersAndGetters();
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
  }

  @Override
  public Serde<Object> getKeySerde()
  {
    return new GenericSerde<>();
  }

  @Override
  public Serde<Object> getValueSerde()
  {
    return new GenericSerde<>();
  }

  /**
   * Create an instance of POJOTimeExtractor
   * @param isStream1 Specifies whether the timeExtractor is for stream1 or not.
   * @return the POJOTimeExtractor
   */
  @Override
  public TimeExtractor getTimeExtractor(boolean isStream1)
  {
    if (isStream1) {
      if (timeFields == null || timeFields.get(0) == null) {
        return null;
      }
      return new POJOTimeExtractor(timeFields.get(0));
    } else {
      if (timeFields == null || timeFields.get(1) == null) {
        return null;
      }
      return new POJOTimeExtractor(timeFields.get(1));
    }
  }

  /**
   * Create an instance of POJOKeyBucketExtractor
   * @param isStream1 Specifies whether the KeyBucketExtractor is for stream1 or not.
   * @return POJOKeyBucketExtractor
   */
  @Override
  public KeyBucketExtractor getKeyBucketExtractor(boolean isStream1)
  {
    return new POJOKeyBucketExtractor();
  }

  /**
   * Returns the streamcodec for the streams
   * @param isStream1data Specifies whether the codec needs for stream1 or stream2.
   * @return the object of JoinStreamCodec
   */
  private StreamCodec<Object> getInnerJoinStreamCodec(boolean isStream1data)
  {
    if (isStream1data) {
      return new JoinStreamCodec(getLeftKeyExpression());
    }
    return new JoinStreamCodec(getRightKeyExpression());
  }

  private class FieldObjectMap
  {
    public Class<?> inputClass;
    public PojoUtils.Getter keyGet;
    public PojoUtils.Getter timeFieldGet;
    public Map<PojoUtils.Getter,PojoUtils.Setter> fieldMap;

    public FieldObjectMap()
    {
      fieldMap = new HashMap<>();
    }
  }

  /**
   * POJOKeyBucketExtractor specifies the key bucket as (o.hashCode() % # of buckets)
   */
  public class POJOKeyBucketExtractor implements KeyBucketExtractor<Object>
  {
    @Override
    public long getBucket(Object o)
    {
      return o.hashCode() % getNoOfBuckets();
    }
  }

  /**
   * Extract the time value from Object
   */
  public class POJOTimeExtractor implements TimeExtractor<Object>
  {
    String timeFieldExpr;
    PojoUtils.Getter<Object, Date> timeFieldGet;

    public POJOTimeExtractor(String timeFieldExpr)
    {
      this.timeFieldExpr = timeFieldExpr;
    }

    @SuppressWarnings("unchecked")
    @Override
    public long getTime(Object o)
    {
      if (timeFieldGet == null) {
        timeFieldGet = PojoUtils.createGetter(o.getClass(), timeFieldExpr, Date.class);
      }
      return timeFieldGet.get(o).getTime();
    }
  }
}

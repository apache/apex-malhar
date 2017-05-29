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
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.window.impl.KeyedWindowedMergeOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedMergeOperatorImpl;
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
 * @deprecated This operator is deprecated and would be removed in the following major release. <br/>
 * Please use {@link WindowedMergeOperatorImpl} or {@link KeyedWindowedMergeOperatorImpl} for join use cases.
 *
 * @since 3.5.0
 */
@Deprecated
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class POJOInnerJoinOperator extends AbstractManagedStateInnerJoinOperator<Object,Object> implements Operator.ActivationListener<Context>
{
  private transient long timeIncrement;
  private transient FieldObjectMap[] inputFieldObjects = (FieldObjectMap[])Array.newInstance(FieldObjectMap.class, 2);
  protected transient Class<?> outputClass;
  private long time = System.currentTimeMillis();

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
    timeIncrement = context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    super.setup(context);
    for (int i = 0; i < 2; i++) {
      inputFieldObjects[i] = new FieldObjectMap();
    }
  }

  /**
   * Extract the time value from the given tuple
   * @param tuple given tuple
   * @param isStream1Data Specifies whether the given tuple belongs to stream1 or not.
   * @return the time in milliseconds
   */
  @Override
  public long extractTime(Object tuple, boolean isStream1Data)
  {
    return timeFields == null ? time : (long)(isStream1Data ? inputFieldObjects[0].timeFieldGet.get(tuple) :
          inputFieldObjects[1].timeFieldGet.get(tuple));
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
    time += timeIncrement;
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
}

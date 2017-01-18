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

import java.util.Arrays;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;

import com.google.common.base.Preconditions;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>
 * An abstract implementation of inner join operator. Operator receives tuples from two streams,
 * applies the join operation based on constraint and emit the joined value.
 * Concrete classes should provide implementation to extractKey, extractTime, mergeTuples methods.
 *
 * <b>Properties:</b><br>
 * <b>includeFieldStr</b>: List of comma separated fields to be added to the output tuple.
 *                         Ex: Field1,Field2;Field3,Field4<br>
 * <b>leftKeyExpression</b>: key field expression for stream1.<br>
 * <b>rightKeyExpression</b>: key field expression for stream2.<br>
 * <b>timeFields</b>: List of comma separated time field for both the streams. Ex: Field1,Field2<br>
 * <b>expiryTime</b>: Expiry time in milliseconds for stored tuples which comes from both streams<br>
 * <b>isLeftKeyPrimary</b>: : Specifies whether the left key(Stream1 key) is primary or not<br>
 * <b>isRightKeyPrimary</b>: : Specifies whether the right key(stream2 key) is primary or not<br>
 *
 * <b> Example: </b> <br>
 *  Left input port receives customer details and right input port receives Order details.
 *  Schema for the Customer be in the form of {ID, Name, CTime}
 *  Schema for the Order be in the form of {OID, CID, OTime}
 *  Now, Join the tuples of Customer and Order streams where Customer.ID = Order.CID and the constraint is
 *  matched tuples must have timestamp within 5 minutes.
 *  Here, leftKeyExpression = ID, rightKeyExpression = CID and Time Fields = CTime,
 *  OTime, expiryTime = 5 minutes </b> <br>
 *
 *  @displayName Abstract Inner Join Operator
 *  @tags join
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractInnerJoinOperator<K,T> extends BaseOperator
{
  @NotNull
  private String leftKeyExpression;
  @NotNull
  private String rightKeyExpression;
  protected transient String[][] includeFields;
  protected transient List<String> keyFieldExpressions;
  protected transient List<String> timeFields;
  @AutoMetric
  private long tuplesJoinedPerSec;
  private double windowTimeSec;
  private int tuplesCount;
  @NotNull
  private String includeFieldStr;
  private String timeFieldsStr;
  @NotNull
  private Long expiryTime;
  private boolean isLeftKeyPrimary = false;
  private boolean isRightKeyPrimary = false;
  protected SpillableComplexComponent component;
  protected Spillable.SpillableListMultimap<K,T> stream1Data;
  protected Spillable.SpillableListMultimap<K,T> stream2Data;

  /**
   * Process the tuple which are received from input ports with the following steps:
   * 1) Extract key from the given tuple
   * 2) Insert <key,tuple> into the store where store is the stream1Data if the tuple
   * receives from stream1 or viceversa.
   * 3) Get the values of the key if found it in opposite store
   * 4) Merge the given tuple and values found from step (3)
   * @param tuple given tuple
   * @param isStream1Data Specifies whether the given tuple belongs to stream1 or not.
   */
  protected void processTuple(T tuple, boolean isStream1Data)
  {
    Spillable.SpillableListMultimap<K,T> store = isStream1Data ? stream1Data : stream2Data;
    K key = extractKey(tuple,isStream1Data);
    if (!store.put(key, tuple)) {
      return;
    }
    Spillable.SpillableListMultimap<K, T> valuestore = isStream1Data ? stream2Data : stream1Data;
    joinStream(tuple,isStream1Data, valuestore.get(key));
  }

  /**
   * Merge the given tuple and list of values.
   * @param tuple given tuple
   * @param isStream1Data Specifies whether the given tuple belongs to stream1 or not.
   * @param value list of tuples
   */
  protected void joinStream(T tuple, boolean isStream1Data, List<T> value)
  {
    // Join the input tuple with the joined tuples
    if (value != null) {
      for (T joinedValue : value) {
        T result = isStream1Data ? mergeTuples(tuple, joinedValue) :
            mergeTuples(joinedValue, tuple);
        if (result != null) {
          tuplesCount++;
          emitTuple(result);
        }
      }
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    component = new SpillableComplexComponentImpl(new InMemSpillableStateStore());
    if (stream1Data == null && stream2Data == null) {
      createStores();
    }
    component.setup(context);
    keyFieldExpressions = Arrays.asList(leftKeyExpression,rightKeyExpression);
    if (timeFields != null) {
      timeFields = Arrays.asList(timeFieldsStr.split(","));
    }
    String[] streamFields = includeFieldStr.split(";");
    includeFields = new String[2][];
    for (int i = 0; i < streamFields.length; i++) {
      includeFields[i] = streamFields[i].split(",");
    }
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    component.beginWindow(windowId);
    tuplesJoinedPerSec = 0;
    tuplesCount = 0;
  }

  @Override
  public void endWindow()
  {
    component.endWindow();
    tuplesJoinedPerSec = (long)(tuplesCount / windowTimeSec);
  }

  @Override
  public void teardown()
  {
    component.teardown();
  }

  /**
   * Extract the key from the given tuple
   * @param tuple given tuple
   * @param isStream1Data Specifies whether the given tuple belongs to stream1 or not.
   * @return the key
   */
  public abstract K extractKey(T tuple, boolean isStream1Data);

  /**
   * Merge the given tuples
   * @param tuple1 tuple belongs to stream1
   * @param tuple2 tuple belongs to stream1
   * @return the merge tuple
   */
  public abstract T mergeTuples(T tuple1, T tuple2);

  /**
   * Emit the given tuple
   * @param tuple given tuple
   */
  public abstract void emitTuple(T tuple);

  /**
   * Create stores for both the streams
   */
  public void createStores()
  {
    stream1Data = component.newSpillableArrayListMultimap(0,null,null);
    stream2Data = component.newSpillableArrayListMultimap(0,null,null);
  }

  /**
   * Get the left key expression
   * @return the leftKeyExpression
   */
  public String getLeftKeyExpression()
  {
    return leftKeyExpression;
  }

  /**
   * Set the left key expression
   * @param leftKeyExpression given leftKeyExpression
   */
  public void setLeftKeyExpression(String leftKeyExpression)
  {
    this.leftKeyExpression = leftKeyExpression;
  }

  /**
   * Get the right key expression
   * @return the rightKeyExpression
   */
  public String getRightKeyExpression()
  {
    return rightKeyExpression;
  }

  /**
   * Set the right key expression
   * @param rightKeyExpression given rightKeyExpression
   */
  public void setRightKeyExpression(String rightKeyExpression)
  {
    this.rightKeyExpression = rightKeyExpression;
  }

  /**
   * Return the include fields of two streams
   * @return the includeFieldStr
   */
  public String getIncludeFieldStr()
  {
    return includeFieldStr;
  }

  /**
   * List of comma separated fields to be added to the output tuple.
   * @param includeFieldStr given includeFieldStr
   */
  public void setIncludeFieldStr(@NotNull String includeFieldStr)
  {
    this.includeFieldStr = Preconditions.checkNotNull(includeFieldStr);
  }

  /**
   * Return the time fields for both the streams
   * @return the timeFieldsStr
   */
  public String getTimeFieldsStr()
  {
    return timeFieldsStr;
  }

  /**
   * Set the time fields as comma separated for both the streams
   * @param timeFieldsStr given timeFieldsStr
   */
  public void setTimeFieldsStr(String timeFieldsStr)
  {
    this.timeFieldsStr = timeFieldsStr;
  }

  /**
   * returns the expiry time
   * @return the expiryTime
   */
  public Long getExpiryTime()
  {
    return expiryTime;
  }

  /**
   * Sets the expiry time
   * @return the expiryTime
   */
  public void setExpiryTime(@NotNull Long expiryTime)
  {
    this.expiryTime = Preconditions.checkNotNull(expiryTime);
  }

  /**
   * return whether the left key is primary or not
   * @return the isLeftKeyPrimary
   */
  public boolean isLeftKeyPrimary()
  {
    return isLeftKeyPrimary;
  }

  /**
   * Set the leftKeyPrimary
   * @param leftKeyPrimary given leftKeyPrimary
   */
  public void setLeftKeyPrimary(boolean leftKeyPrimary)
  {
    isLeftKeyPrimary = leftKeyPrimary;
  }

  /**
   * return whether the right key is primary or not
   * @return the isRightKeyPrimary
   */
  public boolean isRightKeyPrimary()
  {
    return isRightKeyPrimary;
  }

  /**
   * Set the rightKeyPrimary
   * @param rightKeyPrimary given rightKeyPrimary
   */
  public void setRightKeyPrimary(boolean rightKeyPrimary)
  {
    isRightKeyPrimary = rightKeyPrimary;
  }
}

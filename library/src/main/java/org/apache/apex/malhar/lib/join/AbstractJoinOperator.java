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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>
 * This is the base implementation of join operator. Operator receives tuples from two streams,
 * applies the join operation based on constraint and emit the joined value.
 * Subclasses should provide implementation to createOutputTuple,copyValue, getKeyValue, getTime methods.
 *
 * <b>Properties:</b><br>
 * <b>expiryTime</b>: Expiry time for stored tuples<br>
 * <b>includeFieldStr</b>: List of comma separated fields to be added to the output tuple.
 *                         Ex: Field1,Field2;Field3,Field4<br>
 * <b>keyFields</b>: List of comma separated key field for both the streams. Ex: Field1,Field2<br>
 * <b>timeFields</b>: List of comma separated time field for both the streams. Ex: Field1,Field2<br>
 * <b>bucketSpanInMillis</b>: Span of each bucket in milliseconds.<br>
 * <b>strategy</b>: Type of join operation. Default type is inner join<br>
 * <br>
 *
 * <b> Example: </b> <br>
 *  Left input port receives customer details and right input port receives Order details.
 *  Schema for the Customer be in the form of
 *  Schema for the Order be in the form of
 *  Now, Join the tuples of Customer and Order streams where Customer.ID = Order.CID and the constraint is
 *  matched tuples must have timestamp within 5 minutes.
 *  Here, key Fields = ID, CID and Time Fields = RTime, OTime, expiryTime = 5 minutes </b> <br>
 *
 *
 * @displayName Abstract Join Operator
 * @tags join
 *
 * @since 3.4.0
 */
@InterfaceStability.Unstable
public abstract class AbstractJoinOperator<T> extends BaseOperator implements Operator.CheckpointNotificationListener
{
  @AutoMetric
  private long tuplesJoinedPerSec;
  private double windowTimeSec;
  protected int tuplesCount;
  public final transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<>();

  // Strategy of Join operation, by default the option is inner join
  protected JoinStrategy strategy = JoinStrategy.INNER_JOIN;
  // This represents whether the processing tuple is from left port or not
  protected boolean isLeft;

  @InputPortFieldAnnotation
  public transient DefaultInputPort<T> input1 = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      isLeft = true;
      processTuple(tuple);
    }
  };
  @InputPortFieldAnnotation
  public transient DefaultInputPort<T> input2 = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      isLeft = false;
      processTuple(tuple);
    }
  };

  // Stores for each of the input port
  @NotNull
  protected StoreContext leftStore;
  @NotNull
  protected StoreContext rightStore;
  private String includeFieldStr;
  private String keyFieldStr;
  private String timeFieldStr;

  @Override
  public void setup(Context.OperatorContext context)
  {
    // Checks whether the strategy is outer join and set it to store
    boolean isOuter = strategy.equals(JoinStrategy.LEFT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN);
    leftStore.getStore().isOuterJoin(isOuter);
    isOuter = strategy.equals(JoinStrategy.RIGHT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN);
    rightStore.getStore().isOuterJoin(isOuter);
    // Setup the stores
    leftStore.getStore().setup(context);
    rightStore.getStore().setup(context);
    populateFields();
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  /**
   * Create the event with the given tuple. If it successfully inserted it into the store
   * then it does the join operation
   *
   * @param tuple Tuple to process
   */
  protected void processTuple(T tuple)
  {
    JoinStore store = isLeft ? leftStore.getStore() : rightStore.getStore();
    TimeEvent t = createEvent(tuple);
    if (store.put(t)) {
      join(t, isLeft);
    }
  }

  private void populateFields()
  {
    populateIncludeFields();
    populateKeyFields();
    if (timeFieldStr != null) {
      populateTimeFields();
    }
  }

  /**
   * Populate the fields from the includeFiledStr
   */
  private void populateIncludeFields()
  {
    String[] portFields = includeFieldStr.split(";");
    assert (portFields.length == 2);
    leftStore.setIncludeFields(portFields[0].split(","));
    rightStore.setIncludeFields(portFields[1].split(","));
  }

  /**
   * Get the tuples from another store based on join constraint and key
   *
   * @param tuple  input
   * @param isLeft whether the given tuple is from first port or not
   */
  private void join(TimeEvent tuple, boolean isLeft)
  {
    // Get the valid tuples from the store based on key
    // If the tuple is null means the join type is outer and return unmatched tuples from store.

    ArrayList<TimeEvent> value;
    JoinStore store = isLeft ? rightStore.getStore() : leftStore.getStore();

    if (tuple != null) {
      value = (ArrayList<TimeEvent>)store.getValidTuples(tuple);
    } else {
      value = (ArrayList<TimeEvent>)store.getUnMatchedTuples();
    }

    // Join the input tuple with the joined tuples
    if (value != null) {
      List<T> result = new ArrayList<>();
      for (TimeEvent joinedValue : value) {
        T output = createOutputTuple();
        Object tupleValue = null;
        if (tuple != null) {
          tupleValue = tuple.getValue();
        }
        copyValue(output, tupleValue, isLeft);
        copyValue(output, joinedValue.getValue(), !isLeft);
        result.add(output);
        joinedValue.setMatch(true);
      }
      if (tuple != null) {
        tuple.setMatch(true);
      }
      if (result.size() != 0) {
        outputPort.emit(result);
        tuplesCount += result.size();
      }
    }
  }

  // Emit the unmatched tuples, if the strategy is outer join
  @Override
  public void endWindow()
  {
    if (strategy.equals(JoinStrategy.LEFT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN)) {
      join(null, false);
    }
    if (strategy.equals(JoinStrategy.RIGHT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN)) {
      join(null, true);
    }
    leftStore.getStore().endWindow();
    rightStore.getStore().endWindow();
    tuplesJoinedPerSec = (long)(tuplesCount / windowTimeSec);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    tuplesJoinedPerSec = 0;
    tuplesCount = 0;
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
    leftStore.getStore().checkpointed(windowId);
    rightStore.getStore().checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    leftStore.getStore().committed(windowId);
    rightStore.getStore().committed(windowId);
  }

  /**
   * Convert the given tuple to event
   *
   * @param tuple Given tuple to convert into event
   * @return event
   */
  protected TimeEvent createEvent(Object tuple)
  {
    String key = leftStore.getKeys();
    String timeField = leftStore.getTimeFields();
    if (!isLeft) {
      key = rightStore.getKeys();
      timeField = rightStore.getTimeFields();
    }
    if (timeField != null) {
      return new TimeEventImpl(getKeyValue(key, tuple), (Long)getTime(timeField, tuple), tuple);
    } else {
      return new TimeEventImpl(getKeyValue(key, tuple), Calendar.getInstance().getTimeInMillis(), tuple);
    }
  }

  private void populateKeyFields()
  {
    leftStore.setKeys(keyFieldStr.split(",")[0]);
    rightStore.setKeys(keyFieldStr.split(",")[1]);
  }

  public JoinStrategy getStrategy()
  {
    return strategy;
  }

  public void setStrategy(JoinStrategy strategy)
  {
    this.strategy = strategy;
  }

  public void setLeftStore(@NotNull JoinStore lStore)
  {
    leftStore = new StoreContext(lStore);
  }

  public void setRightStore(@NotNull JoinStore rStore)
  {
    rightStore = new StoreContext(rStore);
  }

  public void setKeyFields(String keyFieldStr)
  {
    this.keyFieldStr = keyFieldStr;
  }

  public void setTimeFieldStr(String timeFieldStr)
  {
    this.timeFieldStr = timeFieldStr;
  }

  public void setIncludeFields(String includeFieldStr)
  {
    this.includeFieldStr = includeFieldStr;
  }

  public StoreContext getLeftStore()
  {
    return leftStore;
  }

  public StoreContext getRightStore()
  {
    return rightStore;
  }

  public String getIncludeFieldStr()
  {
    return includeFieldStr;
  }

  public String getKeyFieldStr()
  {
    return keyFieldStr;
  }

  public String getTimeFieldStr()
  {
    return timeFieldStr;
  }

  /**
   * Specify the comma separated time fields for both steams
   */
  private void populateTimeFields()
  {
    leftStore.setTimeFields(timeFieldStr.split(",")[0]);
    rightStore.setTimeFields(timeFieldStr.split(",")[1]);
  }

  public void setStrategy(String policy)
  {
    this.strategy = JoinStrategy.valueOf(policy.toUpperCase());
  }

  /**
   * Create the output object
   *
   * @return output tuple
   */
  protected abstract T createOutputTuple();

  /**
   * Get the values from extractTuple and set these values to the output
   *
   * @param output otuput tuple
   * @param extractTuple Extract the values from this tuple
   * @param isLeft Whether the extracted tuple belongs to left stream or not
   */
  protected abstract void copyValue(T output, Object extractTuple, boolean isLeft);

  /**
   * Get the value of the key field from the given tuple
   *
   * @param keyField Value of the field to extract from given tuple
   * @param tuple Given tuple
   * @return the value of field from given tuple
   */
  protected abstract Object getKeyValue(String keyField, Object tuple);

  /**
   * Get the value of the time field from the given tuple
   *
   * @param field Time field
   * @param tuple given tuple
   * @return the value of time field from given tuple
   */
  protected abstract Object getTime(String field, Object tuple);

  public static enum JoinStrategy
  {
    INNER_JOIN,
    LEFT_OUTER_JOIN,
    RIGHT_OUTER_JOIN,
    OUTER_JOIN
  }

  public static class StoreContext
  {
    private transient String timeFields;
    private transient String[] includeFields;
    private transient String keys;
    private JoinStore store;

    public StoreContext(JoinStore store)
    {
      this.store = store;
    }

    public String getTimeFields()
    {
      return timeFields;
    }

    public void setTimeFields(String timeFields)
    {
      this.timeFields = timeFields;
    }

    public String[] getIncludeFields()
    {
      return includeFields;
    }

    public void setIncludeFields(String[] includeFields)
    {
      this.includeFields = includeFields;
    }

    public String getKeys()
    {
      return keys;
    }

    public void setKeys(String keys)
    {
      this.keys = keys;
    }

    public JoinStore getStore()
    {
      return store;
    }

    public void setStore(JoinStore store)
    {
      this.store = store;
    }
  }
}

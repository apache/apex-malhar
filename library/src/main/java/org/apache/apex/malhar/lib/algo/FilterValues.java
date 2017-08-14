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
package org.apache.apex.malhar.lib.algo;

import java.util.Arrays;
import java.util.HashSet;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator filters the incoming stream of values by the specified set of filter values.
 * <p>
 * Filters incoming stream and emits values as specified by the set of values to filter. If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted. The values are expected to be immutable.
 * </p>
 * <p>
 * This operator should not be used with mutable objects. If this operator has immutable Objects, override "cloneCopy" to ensure a new copy is sent out.
 * This is a pass through node<br>
 * <br>
 * <b>StateFull : No, </b> tuple are processed in current window. <br>
 * <b>Partitions : Yes, </b> no dependency among input tuples. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expect T (a POJO)<br>
 * <b>filter</b>: emits T (a POJO)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>keys</b>: The keys to pass through. Those not in the list are dropped. A comma separated list of keys<br>
 * <br>
 * </p>
 *
 * @displayName Filter Values
 * @category Rules and Alerts
 * @tags filter
 *
 * @since 0.3.2
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class FilterValues<T> extends BaseOperator
{
  /**
   * The input port on which tuples are received.
   */
  public final transient DefaultInputPort<T> data = new DefaultInputPort<T>()
  {
    /**
     * Processes tuple to see if it matches the filter. Emits if at least one key makes the cut
     * By setting inverse as true, match is changed to un-matched
     */
    @Override
    public void process(T tuple)
    {
      boolean contains = values.contains(tuple);
      if ((contains && !inverse) || (!contains && inverse)) {
        filter.emit(cloneValue(tuple));
      }
    }
  };

  /**
   * The output port on which tuples satisfying the filter are emitted.
   */
  public final transient DefaultOutputPort<T> filter = new DefaultOutputPort<T>();

  @NotNull()
  HashSet<T> values = new HashSet<T>();
  boolean inverse = false;

  /**
   * Gets the inverse property.
   * @return inverse
   */
  public boolean getInverse()
  {
    return inverse;
  }

  /**
   * If true then only matches are emitted. If false then only non matches are emitted.
   * @param val
   */
  public void setInverse(boolean val)
  {
    inverse = val;
  }

  /**
   * Adds a value to the filter list
   *
   * @param val adds to filter list
   */
  public void setValue(T val)
  {
    if (val != null) {
      values.add(val);
    }
  }

  /**
   * Adds the list of values to the filter list
   *
   * @param list ArrayList of items to add to filter list
   */
  public void setValues(T[] list)
  {
    if (list != null) {
      values.addAll(Arrays.asList(list));
    }
  }

  /**
   * Gets the values to be filtered.
   * @return The values to be filtered.
   */
  public HashSet<T> getValues()
  {
    return values;
  }

  /**
   * A map containing the values to be filtered. The values are set to be the keys in the map, and the
   * values are set to be null.
   * @param values The values to be filtered.
   */
  public void setValues(HashSet<T> values)
  {
    this.values = values;
  }

  /**
   * Clears the filter
   */
  public void clearValues()
  {
    values.clear();
  }

  /**
   * Clones V object. By default assumes immutable object (i.e. a copy is not made). If object is mutable, override this method
   *
   * @param val object bo be cloned
   * @return cloned Val
   */
  public T cloneValue(T val)
  {
    return val;
  }
}

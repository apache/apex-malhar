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
package org.apache.apex.malhar.lib.multiwindow;

import java.util.Map;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.math.RangeKeyVal;
import org.apache.apex.malhar.lib.util.HighLow;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * A range operator of KeyValPair schema which calculates range across multiple streaming windows.
 * <p>
 * This is an end window operator which emits only at Nth window. <br>
 * <br>
 * <b>StateFull : Yes</b>, computes across multiple windows. <br>
 * <b>Partitions : Yes</b>, high/low are unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>range</b>: emits KeyValPair&lt;K,HighLow&lt;V&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <b>windowSize i.e. N</b>: Number of streaming windows that define application window.<br>
 * <br>
 *
 * @displayName Multi Window Range Key Value
 * @category Stats and Aggregations
 * @tags key value, range, numeric
 * @since 0.3.2
 */
public class MultiWindowRangeKeyVal<K, V extends Number> extends RangeKeyVal<K, V>
{
  /**
   * Number of streaming window after which tuple got emitted.
   */
  @Min(2)
  private int windowSize = 2;
  private long windowCount = 0;

  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
  }

  /**
   * Emits range for each key at application window boundary. If no data is received, no emit is done
   * Clears the internal data before return
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void endWindow()
  {
    boolean emit = (++windowCount) % windowSize == 0;

    if (!emit) {
      return;
    }

    for (Map.Entry<K, V> e: high.entrySet()) {
      HighLow<V> hl = new HighLow<V>();
      hl.setHigh(getValue(e.getValue().doubleValue()));
      hl.setLow(getValue(low.get(e.getKey()).doubleValue())); // cannot be null

      range.emit(new KeyValPair(e.getKey(), hl));
    }
    clearCache();
  }
}


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

import org.apache.apex.malhar.lib.math.SumKeyVal;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * A sum operator of KeyValPair schema which accumulates sum across multiple
 * streaming windows.
 * <p>
 * This is an end window operator which emits only at Nth window. <br>
 * <br>
 * <b>StateFull : Yes, </b> sum is computed across streaming windows.  <br>
 * <b>Partitions : No, </b> sum is not unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>sum</b>: emits KeyValPair&lt;K,V&gt;<br>
 * <b>count</b>: emits KeyValPair&lt;K,Integer&gt;</b><br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: If set to true the key in the filter will block tuple.<br>
 * <b>filterBy</b>: List of keys to filter on.<br>
 * <b>windowSize i.e. N</b>: Number of streaming windows that define application
 * window.<br>
 * <br>
 * @displayName Multi Window Sum Key Value
 * @category Stats and Aggregations
 * @tags key value, sum, numeric
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
public class MultiWindowSumKeyVal<K, V extends Number> extends SumKeyVal<K, V>
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
   * Emit only at the end of windowSize window boundary.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void endWindow()
  {
    boolean emit = (++windowCount) % windowSize == 0;

    if (!emit) {
      return;
    }

    // Emit only at the end of application window boundary.
    boolean dosum = sum.isConnected();

    if (dosum) {
      for (Map.Entry<K, SumEntry> e : sums.entrySet()) {
        K key = e.getKey();
        if (dosum) {
          sum.emit(new KeyValPair(key, getValue(e.getValue().sum.doubleValue())));
        }
      }
    }

    // Clear cumulative sum at the end of application window boundary.
    sums.clear();
  }
}


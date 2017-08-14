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
package org.apache.apex.malhar.lib.testbench;

import java.util.HashMap;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.Sink;

/**
 * A sink implementation, which counts the number of times each tuples is collected and stores the results in a hash map.
 * <p></p>
 * @displayName Hash Count Test Sink
 * @category Test Bench
 * @tags sink, count
 * @since 0.3.2
 */
public class HashTestSink<T> implements Sink<T>
{
  public HashMap<T, MutableInt> map = new HashMap<T, MutableInt>();
  public int count = 0;

  /**
   * clears data
   */
  public void clear()
  {
    this.map.clear();
    this.count = 0;
  }

  public int size()
  {
    return map.size();
  }

  public int getCount(T key)
  {
    int ret = -1;
    MutableInt val = map.get(key);
    if (val != null) {
      ret = val.intValue();
    }
    return ret;
  }

  @Override
  public void put(T tuple)
  {
    this.count++;
    MutableInt val = map.get(tuple);
    if (val == null) {
      val = new MutableInt(0);
      map.put(tuple, val);
    }
    val.increment();
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}

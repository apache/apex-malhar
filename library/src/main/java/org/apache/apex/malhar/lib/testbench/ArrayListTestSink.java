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

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.Sink;

/**
 * A sink implementation to collect expected test results in a HashMap.
 * <p>
 * @displayName ArrayList Test Sink
 * @category Test Bench
 * @tags sink
 * @since 0.3.2
 */
public class ArrayListTestSink<T> implements Sink<T>
{
  public HashMap<Object, MutableInt> map = new HashMap<Object, MutableInt>();
  public int count;

  /**
   * clear data
   */
  public void clear()
  {
    this.map.clear();
    this.count = 0;
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
    @SuppressWarnings("unchecked")
    ArrayList<Object> list = (ArrayList<Object>)tuple;
    for (Object o: list) {
      MutableInt val = map.get(o);
      if (val == null) {
        val = new MutableInt(0);
        map.put(o, val);
      }
      val.increment();
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}

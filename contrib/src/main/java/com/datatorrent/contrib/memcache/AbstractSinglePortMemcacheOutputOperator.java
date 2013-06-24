/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.contrib.memcache;

import com.datatorrent.api.DefaultInputPort;

/**
 * Memcache output adapter operator, which send data to Memcached.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>:TBD
 * <br>
 *
 */
public abstract class AbstractSinglePortMemcacheOutputOperator<T> extends AbstractMemcacheOutputOperator
{
  /**
   * Users need to provide conversion of Tuple to message
   * @param tuple
   */
  public abstract void processTuple(T tuple);

  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple); // This is an abstract call
    }
  };
}

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

import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultOutputPort;
import java.util.Map.Entry;

/**
 * Memcache input adapter single port operator, which get Data from Memcached using spymemcached library.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have one output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method getTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: TBD
 * <br>
 */
public abstract class AbstractSinglePortMemcacheInputOperator<T> extends AbstractMemcacheInputOperator
{
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Any concrete class derived from AbstractSinglePortMemcacheInputOperator has to implement this method
   * so that it knows what type of data it will receive from Memcached
   * It converts a byte message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param message
   */
  public abstract T getTuple(Object o);

  @Override
  public void emitTuple(Object o) {
    T t = getTuple(o);
    if( t != null ) {
      outputPort.emit(t);
    }
  }
}

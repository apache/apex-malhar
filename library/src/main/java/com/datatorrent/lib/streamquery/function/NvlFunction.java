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
package com.datatorrent.lib.streamquery.function;

import java.util.LinkedList;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * An implementation of function NVL which lets you substitute a value when a null
 * value is encountered.
 *
 * The syntax for NVL function is:
 *      NVL (orig_string, replace_with)
 *      orig_string: the string to test for a NULL value
 *      replace_with: the value returned if orig_string is null.
 *      (Note the function will return orig_string if it is not null)
 *
 * For example, with the following table
 *
 *      EMP table
 *      ========
 *      name   salary
 *      ----------------
 *      mike   1000
 *      ben    null
 *      dave   1500
 *      SELECT NVL(salary, 0) FROM emp WHERE name='ben' will return 0.
 *      SELECT NVL(salary, 0) FROM emp WHERE name='dave' will return 1500.
 */
public class NvlFunction<T> extends BaseOperator
{
  /**
   * Array to store column inputs during window.
   */
  private transient LinkedList<T> columns = new LinkedList<>();

  /**
   * Array to store aliases input during window.
   */
  private transient LinkedList<T> aliases = new LinkedList<>();

  /**
   * Helper to process a tuple
   */
  private void processInternal()
  {
    if (aliases.size() > 0 && columns.size() > 0) {
      emit(columns.remove(0), aliases.remove(0));
    }
  }

  /**
   * Column input port
   */
  public final transient DefaultInputPort<T> column = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      columns.add(tuple);
      processInternal();
    }
  };

  /**
   * Alias input port
   */
  public final transient DefaultInputPort<T> alias = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      if (tuple == null) {
        columns.removeLast();
        errordata.emit("Error(null alias not allowed)");
        return;
      }
      aliases.add(tuple);
      processInternal();
    }
  };

  /**
   * Output port
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<T> out = new DefaultOutputPort<>();

  public void emit(T column, T alias)
  {
    out.emit(column == null ? alias : column);
  }

  /**
   * Error data output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<String> errordata = new DefaultOutputPort<>();

  @Override
  public void endWindow()
  {
    if (columns.size() != 0 || aliases.size() != 0) {
      throw new IllegalArgumentException("Number of column values mismatches number of aliases");
    }
  }
}

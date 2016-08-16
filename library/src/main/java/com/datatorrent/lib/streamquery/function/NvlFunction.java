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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

import java.util.ArrayList;

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
 *
 * @displayName NVL Function
 * @category Stream Manipulators
 * @tags sql nvl
 * @since 0.3.4
 */
public class NvlFunction<T> extends BaseOperator
{
    /**
     * Array to store column inputs during window.
     */
    private ArrayList<T> columns = new ArrayList<T>();

    /**
     * Array to store aliases input during window.
     */
    private ArrayList<T> aliases = new ArrayList<T>();

    /**
     * Number of input pairs processed in current window.
     */
    private int index = 0;

    /**
     * Helper to process a tuple
     */
    private void processInternal(T tuple, boolean isAlias) {
        int size;
        if (isAlias) {
            aliases.add(tuple);
            size = columns.size();
        }
        else {
            columns.add(tuple);
            size = aliases.size();
        }

        if (size > index) {
            int loc = aliases.size();
            if (loc > columns.size()) {
                loc = columns.size();
            }
            emit(columns.get(loc - 1), aliases.get(loc - 1));
            index++;
        }
    }

    /**
     * Column input port
     */
    public final transient DefaultInputPort<T> column = new DefaultInputPort<T>()
    {
        @Override
        public void process(T tuple) {
            processInternal(tuple, false);
        }
    };

    /**
     * Alias input port
     */
    public final transient DefaultInputPort<T> alias = new DefaultInputPort<T>()
    {
        @Override
        public void process(T tuple) {
            if (tuple == null) {
                errordata.emit("Error(null alias not allowed)");
                return;
            }
            processInternal(tuple, true);
        }
    };

    /**
     * Output port
     */
    @OutputPortFieldAnnotation(optional = true)
    public final transient DefaultOutputPort<T> out = new DefaultOutputPort<T>();

    public void emit(T column, T alias)
    {
        out.emit(column == null ? alias : column);
    }

    /**
     * Error data output port.
     */
    @OutputPortFieldAnnotation(optional = true)
    public final transient DefaultOutputPort<String> errordata = new DefaultOutputPort<String>();

    @Override
    public void endWindow()
    {
        columns.clear();
        aliases.clear();
        index = 0;
    }
}

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

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class takes a HashMap tuple as input from each of the input port. Operator joines the input tuples
 * based on join constraint and emit the result.
 *
 * <br>
 * <b>Ports : </b> <br>
 * <b> input1 : </b> Input port for stream 1, expects HashMap&lt;String, Object&gt; <br>
 * <b> input2 : </b> Input port for stream 2, expects HashMap&lt;String, Object&gt; <br>
 * <b> outputPort: </b> Output port emits ArrayList&lt;HashMap&lt;String, Object&gt;&gt; <br>
 * <br>
 * <b>Example:</b>
 * Input tuple from port1 is
 * {timestamp = 5000, productId = 3, customerId = 108, regionId = 4, amount = $560 }
 *
 * Input tuple from port2 is
 * { timestamp = 5500, productCategory = 8, productId=3 }
 *
 * <b>Properties: </b>
 * <b>expiryTime</b>: 1000<br>
 * <b>includeFieldStr</b>: timestamp, customerId, amount; productCategory, productId<br>
 * <b>keyFields</b>: productId, productId<br>
 * <b>timeFields</b>: timestamp, timestamp<br>
 * <b>bucketSpanInMillis</b>: 500<br>
 *
 * <b>Output</b>
 * { timestamp = 5000, customerId = 108, amount = $560, productCategory = 8, productId=3}
 *
 * @displayName MapJoin Operator
 * @category join
 * @tags join
 *
 * @since 3.4.0
 */
@InterfaceStability.Unstable
public class MapJoinOperator extends AbstractJoinOperator<Map<String, Object>>
{
  @Override
  protected Map<String, Object> createOutputTuple()
  {
    return new HashMap<String, Object>();
  }

  @Override
  protected void copyValue(Map<String, Object> output, Object extractTuple, boolean isLeft)
  {
    String[] fields;
    if (isLeft) {
      fields = leftStore.getIncludeFields();
    } else {
      fields = rightStore.getIncludeFields();
    }
    for (int i = 0; i < fields.length; i++) {
      Object value = null;
      if (extractTuple != null) {
        value = ((Map<String, Object>)extractTuple).get(fields[i]);
      }
      output.put(fields[i], value);
    }
  }

  public Object getKeyValue(String keyField, Object tuple)
  {
    Map<String, Object> o = (Map<String, Object>)tuple;
    return o.get(keyField);
  }

  @Override
  protected Object getTime(String field, Object tuple)
  {
    if (getTimeFieldStr() != null) {
      Map<String, Object> o = (Map<String, Object>)tuple;
      return o.get(field);
    }
    return Calendar.getInstance().getTimeInMillis();
  }
}

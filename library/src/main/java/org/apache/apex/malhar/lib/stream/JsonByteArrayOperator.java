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
package org.apache.apex.malhar.lib.stream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * An implementation of BaseOperator that takes a json byte stream and emits a HashMap of key values.
 * <p>
 * This is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects json byte array &lt;K,V&gt;<br>
 * <b>outputMap</b>: emits HashMap&lt;String,Object&gt;<br>
 * <b>outputJsonObject</b>: emits JSONObject<br>
 * <b>outputFlatMap</b>: emits HashMap&lt;String,Object&gt;<br>
 * &nbsp&nbsp The key will be dot concatenated nested key names <br>
 * &nbsp&nbsp eg: key: "agentinfo.os.name", value: "Ubuntu" <br>
 * <br>
 * @displayName JSON Byte Array
 * @category Tuple Converters
 * @tags json, byte array
 * @since 0.9.4
 */
@Stateless
public class JsonByteArrayOperator extends BaseOperator
{
  private Character CONCAT_CHAR = '_';
  private static final Logger logger = LoggerFactory.getLogger(JsonByteArrayOperator.class);
  /**
   * Input byte array port.
   */
  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    private void getFlatMap(JSONObject jSONObject, Map<String, Object> map, String keyPrefix) throws Exception
    {
      Iterator<String> iterator = jSONObject.keys();
      while (iterator.hasNext()) {
        String key = iterator.next();
        String insertKey = (keyPrefix == null) ? key : keyPrefix + CONCAT_CHAR + key;

        JSONObject value = jSONObject.optJSONObject(key);
        if (value == null) {
          map.put(insertKey, jSONObject.get(key));
        } else {
          getFlatMap(value, map, insertKey);
        }
      }
    }

    @Override
    public void process(byte[] message)
    {
      String inputString = new String(message);
      try {
        JSONObject jSONObject = new JSONObject(inputString);

        // output JSONObject
        outputJsonObject.emit(jSONObject);

        // output map retaining the tree structure from json
        if (outputMap.isConnected()) {
          Iterator<String> iterator = jSONObject.keys();
          HashMap<String, Object> map = new HashMap<String, Object>();
          while (iterator.hasNext()) {
            String key = iterator.next();
            map.put(key, jSONObject.getString(key));
          }
          outputMap.emit(map);
        }

        // output map as flat key value pairs
        if (outputFlatMap.isConnected()) {
          HashMap<String, Object> flatMap = new HashMap<String, Object>();
          getFlatMap(jSONObject, flatMap, null);
          outputFlatMap.emit(flatMap);
        }

      } catch (Throwable ex) {
        DTThrowable.rethrow(ex);
      }
    }

  };

  public void setConcatenationCharacter(char c)
  {
    JsonByteArrayOperator.this.CONCAT_CHAR = c;
  }

  /**
   * Output hash map port.
   */
  public final transient DefaultOutputPort<HashMap<String, Object>> outputMap = new DefaultOutputPort<HashMap<String, Object>>();
  /**
   * Output JSONObject port.
   */
  public final transient DefaultOutputPort<JSONObject> outputJsonObject = new DefaultOutputPort<JSONObject>();
  /**
   * Output hash map port.
   */
  public final transient DefaultOutputPort<HashMap<String, Object>> outputFlatMap = new DefaultOutputPort<HashMap<String, Object>>();
}

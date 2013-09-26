/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 * limitations under the License.
 */package com.datatorrent.lib.stream;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a json byte stream and emits a HashMap of key values
 * <p>
 * This is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects json byte array &lt;K,V&gt;<br>
 * <b>outputMap</b>: emits HashMap&lt;String,String&gt;<br>
 * <br>
 *
 * @since 0.3.5
 */
public class JsonByteArrayToHashMapOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(JsonByteArrayToHashMapOperator.class);
	/**
	 * Input byte array port.
	 */
	@InputPortFieldAnnotation(name = "input")
	public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
	{
    @Override
    public void process(byte[] message)
    {
      String inputString = new String(message);
      try {
        JSONObject jSONObject = new JSONObject(inputString);
        Iterator<String> iterator = jSONObject.keys();
        HashMap<String,String> map = new HashMap<String, String>();
        while(iterator.hasNext()){
          String key = iterator.next();
          map.put(key, jSONObject.getString(key));
        }

        outputMap.emit(map);
      }
      catch (JSONException ex) {
        logger.error(ex.getMessage());
      }
    }
	};

	/**
	 * Output hash map port.
	 */
	@OutputPortFieldAnnotation(name = "map")
	public final transient DefaultOutputPort<HashMap<String, String>> outputMap = new DefaultOutputPort<HashMap<String, String>>();
}

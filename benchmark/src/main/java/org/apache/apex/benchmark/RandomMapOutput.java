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
package org.apache.apex.benchmark;

import java.util.HashMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Operator that outputs random values in a map.
 *
 * @since 1.0.4
 */
public class RandomMapOutput extends BaseOperator
{

  public final transient DefaultOutputPort<HashMap<String, Object>> map_data =
      new DefaultOutputPort<HashMap<String, Object>>();
  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      HashMap<String, Object> map = new HashMap<String, Object>();
      map.put(key, tuple);
      RandomMapOutput.this.process(map);
    }
  };

  private String key;

  public String getKey()
  {
    return key;
  }

  public void setKey(String key)
  {
    this.key = key;
  }

  public void process(HashMap<String, Object> tuple)
  {

    if (map_data.isConnected()) {
      map_data.emit(tuple);
    }
  }
}

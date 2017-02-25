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
package org.apache.apex.examples.mrmonitor;

import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * <p>MapToMRObjectOperator class.</p>
 *
 * @since 0.9.0
 */
public class MapToMRObjectOperator implements Operator
{

  public final transient DefaultInputPort<Map<String, String>> input = new DefaultInputPort<Map<String, String>>()
  {
    @Override
    public void process(Map<String, String> tuple)
    {
      MRStatusObject mrStatusObj = new MRStatusObject();

      for (Map.Entry<String, String> e : tuple.entrySet()) {
        if (e.getKey().equals(Constants.QUERY_KEY_COMMAND)) {
          mrStatusObj.setCommand(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_API_VERSION)) {
          mrStatusObj.setApiVersion(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_APP_ID)) {
          mrStatusObj.setAppId(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_HADOOP_VERSION)) {
          mrStatusObj.setHadoopVersion(Integer.parseInt(e.getValue()));
        } else if (e.getKey().equals(Constants.QUERY_HOST_NAME)) {
          mrStatusObj.setUri(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_HS_PORT)) {
          mrStatusObj.setHistoryServerPort(Integer.parseInt(e.getValue()));
        } else if (e.getKey().equals(Constants.QUERY_JOB_ID)) {
          mrStatusObj.setJobId(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_RM_PORT)) {
          mrStatusObj.setRmPort(Integer.parseInt(e.getValue()));
        }
      }
      output.emit(mrStatusObj);

    }
  };

  public final transient DefaultOutputPort<MRStatusObject> output = new DefaultOutputPort<MRStatusObject>();

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

}

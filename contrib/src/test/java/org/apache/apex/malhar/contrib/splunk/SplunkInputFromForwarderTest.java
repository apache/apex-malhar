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
package org.apache.apex.malhar.contrib.splunk;

import java.io.IOException;
import java.util.Properties;

public class SplunkInputFromForwarderTest
{
  private static class InputFromForwarder extends SplunkInputFromForwarder<String>
  {
    @Override
    public String getMessage(String line)
    {
      return line;
    }
  }

  public static void main(String[] args)
  {
    InputFromForwarder iff = new InputFromForwarder();
    Properties props = new Properties();

    props.put("metadata.broker.list", "127.0.0.1:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    iff.setConfigProperties(props);
    iff.setPort(6789);
    iff.setTopic("integer_values");
    try {
      iff.startServer();
      iff.process();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        iff.startServer();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
